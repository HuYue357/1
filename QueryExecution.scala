package org.apache.spark.sql.execution

import java.io.{BufferedWriter, OutputStreamWriter}
import java.util.UUID

import org.apache.hadoop.fs.Path

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.{InternalRow, QueryPlanningTracker}
import org.apache.spark.sql.catalyst.analysis.UnsupportedOperationChecker
import org.apache.spark.sql.catalyst.expressions.codegen.ByteCodeStats
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, ReturnAnswer}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.StringUtils.PlanStringConcat
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.execution.adaptive.{AdaptiveExecutionContext, InsertAdaptiveSparkPlan}
import org.apache.spark.sql.execution.dynamicpruning.PlanDynamicPruningFilters
import org.apache.spark.sql.execution.exchange.{EnsureRequirements, ReuseExchange}
import org.apache.spark.sql.execution.streaming.{IncrementalExecution, OffsetSeqMetadata}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.util.Utils


import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

/**
 * The primary workflow for executing relational queries using Spark.  Designed to allow easy
 * access to the intermediate phases of query execution for developers.
 *
 * While this is not a public class, we should avoid changing the function names for the sake of
 * changing them, because a lot of developers use the feature for debugging.
 */
class QueryExecution(
    val sparkSession: SparkSession,
    val logical: LogicalPlan,
    val tracker: QueryPlanningTracker = new QueryPlanningTracker) {

  // TODO: Move the planner an optimizer into here from SessionState.
  protected def planner = sparkSession.sessionState.planner

  def assertAnalyzed(): Unit = analyzed

  def assertSupported(): Unit = {
    if (sparkSession.sessionState.conf.isUnsupportedOperationCheckEnabled) {
      UnsupportedOperationChecker.checkForBatch(analyzed)
    }
  }

  lazy val analyzed: LogicalPlan = executePhase(QueryPlanningTracker.ANALYSIS) {
    // We can't clone `logical` here, which will reset the `_analyzed` flag.
    sparkSession.sessionState.analyzer.executeAndCheck(logical, tracker)
  }

  lazy val withCachedData: LogicalPlan = sparkSession.withActive {
    assertAnalyzed()
    assertSupported()
    // clone the plan to avoid sharing the plan instance between different stages like analyzing,
    // optimizing and planning.
    sparkSession.sharedState.cacheManager.useCachedData(analyzed.clone())
  }

  lazy val optimizedPlan: LogicalPlan = executePhase(QueryPlanningTracker.OPTIMIZATION) {
    // clone the plan to avoid sharing the plan instance between different stages like analyzing,
    // optimizing and planning.
    sparkSession.sessionState.optimizer.executeAndTrack(withCachedData.clone(), tracker)
  }

  private def assertOptimized(): Unit = optimizedPlan

  //--------------------------Modified part--------------------------------
  lazy val sparkPlan: SparkPlan = {
    assertOptimized()
    executePhase(QueryPlanningTracker.PLANNING) {
      val clonedPlan = optimizedPlan.clone()

      //Generate root physical candidates
      val candidates: Seq[SparkPlan] = planner.plan(ReturnAnswer(clonedPlan)).toSeq

      //Force Assessment
      executeAllSubqueryCombinations()

      //Pick the first one by default
      candidates.head
    }
  }

  //Force Assessment 
  def executeAllSubqueryCombinations(): Unit = {
    //Generate all root physical candidates from optimizedPlan
    val rootCandidates: Seq[SparkPlan] = planner.plan(ReturnAnswer(optimizedPlan)).toSeq
    if (rootCandidates.isEmpty) return

    rootCandidates.foreach { root =>
      //Grab all PlanLater placeholders and their lp instances in the root candidate
      val placeholders: Seq[(SparkPlan, LogicalPlan)] =
        root.collect { case p @ PlanLater(lp) => (p, lp) }

      if (placeholders.isEmpty) {
        val prepared = QueryExecution.prepareForExecution(preparations, root)
        timeAndRecord(Map.empty, prepared)
      } else {
        //Generate physical candidates for each occupied lp
        val perLpCandidates: Map[LogicalPlan, Seq[SparkPlan]] =
          placeholders.map { case (_, lp) =>
            lp -> planner.plan(ReturnAnswer(lp)).toSeq
          }.toMap

        // Fix order
        val lpsInOrder: Seq[LogicalPlan] = placeholders.map(_._2)
        val allCombos: Seq[Seq[SparkPlan]] = cartesianSeq(lpsInOrder.map(perLpCandidates))

        //Limit combinatorial explosion (unlimited = 0) 
        val maxCombos: Int =
          sparkSession.conf.get("spark.gnn.training.maxCombos", "0").toInt 
        val combosIter = if (maxCombos > 0) allCombos.take(maxCombos) else allCombos

        combosIter.foreach { combo =>
          val mapping: Map[LogicalPlan, SparkPlan] = lpsInOrder.zip(combo).toMap
          // Replace PlanLater(lp) with the selected subtree at the physical level

          val stitched: SparkPlan = injectChoicesIntoRoot(root, mapping)
          // Preparation stage

          val prepared: SparkPlan = QueryExecution.prepareForExecution(preparations, stitched)
          timeAndRecord(mapping, prepared)
        }
      }
    }
  }

  //Injection at the physical plan level: replace PlanLater(lp) in the root with the physical subtree specified by mapping
  private def injectChoicesIntoRoot(
      root: SparkPlan,
      choices: Map[LogicalPlan, SparkPlan]): SparkPlan = {
    root.transformUp {
      case p @ PlanLater(lp) if choices.contains(lp) => choices(lp)
    }
  }
  private def isMeaningfulForTraining(plan: SparkPlan): Boolean = {
    def name(p: SparkPlan): String =
      try p.nodeName.toLowerCase(java.util.Locale.ROOT)
      catch { case _: Throwable => p.getClass.getSimpleName.toLowerCase(java.util.Locale.ROOT) }

    def isCommandLike(p: SparkPlan): Boolean = {
      val n = name(p)
      n.contains("command") || n.contains("setcatalog") || n.contains("namespace")
    }
    def isDataOp(p: SparkPlan): Boolean = {
      val n = name(p)
      n.contains("scan") || n.contains("join") || n.contains("aggregate") ||
        n == "sort" || n == "exchange" || n == "project" || n == "filter"
    }

    val hasStructure     = plan.children.nonEmpty
    val hasDataOperators = plan.find(isDataOp).isDefined
    val looksLikeCommand = plan.find(isCommandLike).isDefined || (plan.children.isEmpty && plan.output.isEmpty)

    hasStructure && hasDataOperators && !looksLikeCommand
  }

  private def timeAndRecord(mapping: Map[LogicalPlan, SparkPlan], prepared: SparkPlan): Unit = {
    //Filter plans not used for training
    if (!isMeaningfulForTraining(prepared)) return

    val start = System.nanoTime()
    var runtimeMs = Double.NaN
    try {
      prepared.execute().count()  
      runtimeMs = (System.nanoTime() - start) / 1e6     //ms
    } catch {
      case _: Exception => runtimeMs = Double.NaN
    } finally {
      saveGraphSample(prepared, runtimeMs)
    }
  }



  // Cartesian product
  private def cartesianSeq[T](lists: Seq[Seq[T]]): Seq[Seq[T]] = {
    lists.foldLeft(Seq(Seq.empty[T])) { (acc, xs) =>
      for {
        a <- acc
        x <- xs
      } yield a :+ x
    }
  }
  // GNN training sample serialization -------------------

  private case class GNode(
    id: Int,
    op: String,
    outCols: Int,
    supportsColumnar: Boolean,
    partitioning: String,
    orderingSize: Int,
    joinType: Option[String],
    buildSide: Option[String],
    hashKeys: Int,
    sortKeys: Int
  )

  private case class GEdge(src: Int, dst: Int, pos: Int)

  private case class GSample(
    nodes: Seq[GNode],
    edges: Seq[GEdge],
    rootPlan: String,
    runtimeMs: Double
  )

  private lazy val jsonMapper: ObjectMapper = {
    val m = new ObjectMapper()
    m.registerModule(DefaultScalaModule)
    m
  }

  private case class JoinFeat(joinType: Option[String], buildSide: Option[String], hashKeys: Int, sortKeys: Int)
  private def joinFeat(p: SparkPlan): JoinFeat = p match {
    case bhj: org.apache.spark.sql.execution.joins.BroadcastHashJoinExec =>
      JoinFeat(
        joinType   = Some(bhj.joinType.toString),   
        buildSide  = Some(bhj.buildSide.toString),
        hashKeys   = bhj.leftKeys.size + bhj.rightKeys.size,
        sortKeys   = 0
      )
    case smj: org.apache.spark.sql.execution.joins.SortMergeJoinExec =>
      JoinFeat(
        joinType   = Some(smj.joinType.toString),   
        buildSide  = None,
        hashKeys   = 0,
        sortKeys   = smj.leftKeys.size + smj.rightKeys.size
      )
    case _ =>
      JoinFeat(None, None, 0, 0)
  }


  //Encode the prepared SparkPlan into a graph structure, ensuring that the id is unique and the edge references are correct

  private def buildGraph(plan: SparkPlan): (Seq[GNode], Seq[GEdge], String) = {

    // Use IdentityHashMap to ensure that the same node is assigned only one id
    val idOf = new java.util.IdentityHashMap[SparkPlan, java.lang.Integer]()
    var nextId = 0
    def id(p: SparkPlan): Int = {
      val got = idOf.get(p)
      if (got != null) got.intValue()
      else {
        val nid = nextId; nextId += 1
        idOf.put(p, java.lang.Integer.valueOf(nid))
        nid
      }
    }

    val visited = new java.util.IdentityHashMap[SparkPlan, java.lang.Boolean]()
    val nodesBuf = scala.collection.mutable.ArrayBuffer[GNode]()
    val edgesBuf = scala.collection.mutable.ArrayBuffer[GEdge]()

    def partitioningString(p: SparkPlan): String =
      try p.outputPartitioning.toString catch { case _: Throwable => "unknown" }
    def orderingSize(p: SparkPlan): Int =
      try p.outputOrdering.size catch { case _: Throwable => 0 }

    def dfs(p: SparkPlan): Unit = {
      if (visited.containsKey(p)) return
      visited.put(p, java.lang.Boolean.TRUE)

      val pid = id(p)


      //Record the node itself
      val jf = joinFeat(p)
      nodesBuf += GNode(
        id               = pid,
        op               = p.nodeName,              
        outCols          = p.output.size,
        supportsColumnar = p.supportsColumnar,
        partitioning     = partitioningString(p),
        orderingSize     = orderingSize(p),
        joinType         = jf.joinType,
        buildSide        = jf.buildSide,
        hashKeys         = jf.hashKeys,
        sortKeys         = jf.sortKeys
      )

      //Processing child nodes and edges
      p.children.zipWithIndex.foreach { case (c, idx) =>
        val cid = id(c)
        edgesBuf += GEdge(src = pid, dst = cid, pos = idx)
        dfs(c)
      }
    }
    dfs(plan)

    //Verification: id is continuous and edges are within the range
    val maxId = nextId - 1
    val idsOk = nodesBuf.forall(n => n.id >= 0 && n.id <= maxId)
    val edgesOk = edgesBuf.forall(e => e.src >= 0 && e.src <= maxId && e.dst >= 0 && e.dst <= maxId)
    if (!idsOk || !edgesOk) {
      //Throw an error or discard the sample if an exception occurs

      throw new IllegalStateException(s"Graph ID/edge out of range: maxId=$maxId nodes=${nodesBuf.size} edges=${edgesBuf.size}")
    }

    val rootStr = try plan.simpleString(2000) catch { case _: Throwable => plan.nodeName }
    (nodesBuf.toSeq, edgesBuf.toSeq, rootStr)
  }

  //Write JSONL
  private def saveGraphSample(prepared: SparkPlan, runtimeMs: Double): Unit = {
    val (nodes, edges, rootStr) = buildGraph(prepared)
    val sample = GSample(nodes, edges, rootStr, runtimeMs)
    val json = jsonMapper.writeValueAsString(sample)
    val fw = new java.io.FileWriter("/tmp/gnn_training.jsonl", true)
    try fw.write(json + "\n") finally fw.close()
  }
  //------------------------------------Modification completed------------------------------------------------

  // executedPlan should not be used to initialize any SparkPlan. It should be
  // only used for execution.
  lazy val executedPlan: SparkPlan = {
    // We need to materialize the optimizedPlan here, before tracking the planning phase, to ensure
    // that the optimization time is not counted as part of the planning phase.
    assertOptimized()
    executePhase(QueryPlanningTracker.PLANNING) {
      // clone the plan to avoid sharing the plan instance between different stages like analyzing,
      // optimizing and planning.
      QueryExecution.prepareForExecution(preparations, sparkPlan.clone())
    }
  }

  lazy val toRdd: RDD[InternalRow] = new SQLExecutionRDD(
    executedPlan.execute(), sparkSession.sessionState.conf)

  /** Get the metrics observed during the execution of the query plan. */
  def observedMetrics: Map[String, Row] = CollectMetricsExec.collect(executedPlan)

  protected def preparations: Seq[Rule[SparkPlan]] = {
    QueryExecution.preparations(sparkSession,
      Option(InsertAdaptiveSparkPlan(AdaptiveExecutionContext(sparkSession, this))))
  }

  private def executePhase[T](phase: String)(block: => T): T = sparkSession.withActive {
    tracker.measurePhase(phase)(block)
  }

  def simpleString: String = simpleString(false)

  def simpleString(formatted: Boolean): String = withRedaction {
    val concat = new PlanStringConcat()
    concat.append("== Physical Plan ==\n")
    if (formatted) {
      try {
        ExplainUtils.processPlan(executedPlan, concat.append)
      } catch {
        case e: AnalysisException => concat.append(e.toString)
        case e: IllegalArgumentException => concat.append(e.toString)
      }
    } else {
      QueryPlan.append(executedPlan, concat.append, verbose = false, addSuffix = false)
    }
    concat.append("\n")
    concat.toString
  }

  def explainString(mode: ExplainMode): String = {
    val queryExecution = if (logical.isStreaming) {
      // This is used only by explaining `Dataset/DataFrame` created by `spark.readStream`, so the
      // output mode does not matter since there is no `Sink`.
      new IncrementalExecution(
        sparkSession, logical, OutputMode.Append(), "<unknown>",
        UUID.randomUUID, UUID.randomUUID, 0, OffsetSeqMetadata(0, 0))
    } else {
      this
    }

    mode match {
      case SimpleMode =>
        queryExecution.simpleString
      case ExtendedMode =>
        queryExecution.toString
      case CodegenMode =>
        try {
          org.apache.spark.sql.execution.debug.codegenString(queryExecution.executedPlan)
        } catch {
          case e: AnalysisException => e.toString
        }
      case CostMode =>
        queryExecution.stringWithStats
      case FormattedMode =>
        queryExecution.simpleString(formatted = true)
    }
  }

  private def writePlans(append: String => Unit, maxFields: Int): Unit = {
    val (verbose, addSuffix) = (true, false)
    append("== Parsed Logical Plan ==\n")
    QueryPlan.append(logical, append, verbose, addSuffix, maxFields)
    append("\n== Analyzed Logical Plan ==\n")
    try {
      append(
        truncatedString(
          analyzed.output.map(o => s"${o.name}: ${o.dataType.simpleString}"), ", ", maxFields)
      )
      append("\n")
      QueryPlan.append(analyzed, append, verbose, addSuffix, maxFields)
      append("\n== Optimized Logical Plan ==\n")
      QueryPlan.append(optimizedPlan, append, verbose, addSuffix, maxFields)
      append("\n== Physical Plan ==\n")
      QueryPlan.append(executedPlan, append, verbose, addSuffix, maxFields)
    } catch {
      case e: AnalysisException => append(e.toString)
    }
  }

  override def toString: String = withRedaction {
    val concat = new PlanStringConcat()
    writePlans(concat.append, SQLConf.get.maxToStringFields)
    concat.toString
  }

  def stringWithStats: String = withRedaction {
    val concat = new PlanStringConcat()
    val maxFields = SQLConf.get.maxToStringFields

    // trigger to compute stats for logical plans
    try {
      optimizedPlan.stats
    } catch {
      case e: AnalysisException => concat.append(e.toString + "\n")
    }
    // only show optimized logical plan and physical plan
    concat.append("== Optimized Logical Plan ==\n")
    QueryPlan.append(optimizedPlan, concat.append, verbose = true, addSuffix = true, maxFields)
    concat.append("\n== Physical Plan ==\n")
    QueryPlan.append(executedPlan, concat.append, verbose = true, addSuffix = false, maxFields)
    concat.append("\n")
    concat.toString
  }

  /**
   * Redact the sensitive information in the given string.
   */
  private def withRedaction(message: String): String = {
    Utils.redact(sparkSession.sessionState.conf.stringRedactionPattern, message)
  }

  /** A special namespace for commands that can be used to debug query execution. */
  // scalastyle:off
  object debug {
  // scalastyle:on

    /**
     * Prints to stdout all the generated code found in this plan (i.e. the output of each
     * WholeStageCodegen subtree).
     */
    def codegen(): Unit = {
      // scalastyle:off println
      println(org.apache.spark.sql.execution.debug.codegenString(executedPlan))
      // scalastyle:on println
    }

    /**
     * Get WholeStageCodegenExec subtrees and the codegen in a query plan
     *
     * @return Sequence of WholeStageCodegen subtrees and corresponding codegen
     */
    def codegenToSeq(): Seq[(String, String, ByteCodeStats)] = {
      org.apache.spark.sql.execution.debug.codegenStringSeq(executedPlan)
    }

    /**
     * Dumps debug information about query execution into the specified file.
     *
     * @param maxFields maximum number of fields converted to string representation.
     */
    def toFile(path: String, maxFields: Int = Int.MaxValue): Unit = {
      val filePath = new Path(path)
      val fs = filePath.getFileSystem(sparkSession.sessionState.newHadoopConf())
      val writer = new BufferedWriter(new OutputStreamWriter(fs.create(filePath)))
      val append = (s: String) => {
        writer.write(s)
      }
      try {
        writePlans(append, maxFields)
        writer.write("\n== Whole Stage Codegen ==\n")
        org.apache.spark.sql.execution.debug.writeCodegen(writer.write, executedPlan)
      } finally {
        writer.close()
      }
    }
  }
}

object QueryExecution {

  private[execution] def preparations(
      sparkSession: SparkSession,
      adaptiveExecutionRule: Option[InsertAdaptiveSparkPlan] = None): Seq[Rule[SparkPlan]] = {
    // `AdaptiveSparkPlanExec` is a leaf node. If inserted, all the following rules will be no-op
    // as the original plan is hidden behind `AdaptiveSparkPlanExec`.
    adaptiveExecutionRule.toSeq ++
    Seq(
      PlanDynamicPruningFilters(sparkSession),
      PlanSubqueries(sparkSession),
      EnsureRequirements(sparkSession.sessionState.conf),
      ApplyColumnarRulesAndInsertTransitions(sparkSession.sessionState.conf,
        sparkSession.sessionState.columnarRules),
      CollapseCodegenStages(sparkSession.sessionState.conf),
      ReuseExchange(sparkSession.sessionState.conf),
      ReuseSubquery(sparkSession.sessionState.conf)
    )
  }

  /**
   * Prepares a planned [[SparkPlan]] for execution by inserting shuffle operations and internal
   * row format conversions as needed.
   */
  private[execution] def prepareForExecution(
      preparations: Seq[Rule[SparkPlan]],
      plan: SparkPlan): SparkPlan = {
    preparations.foldLeft(plan) { case (sp, rule) => rule.apply(sp) }
  }

  /**
   * Transform a [[LogicalPlan]] into a [[SparkPlan]].
   *
   * Note that the returned physical plan still needs to be prepared for execution.
   */
  def createSparkPlan(
      sparkSession: SparkSession,
      planner: SparkPlanner,
      plan: LogicalPlan): SparkPlan = {
    // TODO: We use next(), i.e. take the first plan returned by the planner, here for now,
    //       but we will implement to choose the best plan.
    planner.plan(ReturnAnswer(plan)).next()
  }

  /**
   * Prepare the [[SparkPlan]] for execution.
   */
  def prepareExecutedPlan(spark: SparkSession, plan: SparkPlan): SparkPlan = {
    prepareForExecution(preparations(spark), plan)
  }

  /**
   * Transform the subquery's [[LogicalPlan]] into a [[SparkPlan]] and prepare the resulting
   * [[SparkPlan]] for execution.
   */
  def prepareExecutedPlan(spark: SparkSession, plan: LogicalPlan): SparkPlan = {
    val sparkPlan = createSparkPlan(spark, spark.sessionState.planner, plan.clone())
    prepareExecutedPlan(spark, sparkPlan)
  }
}
