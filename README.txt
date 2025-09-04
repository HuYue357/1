TPC-DS operational specifications

This Spark cluster is based on Docker Desktop and contains one node.
Run the following command in the Windows cmd box to enter the container:
           
  docker exec -it master bash –login

Run steps
Start the service:

  hdfs namenode &
  hdfs datanode &
  hdfs secondarynamenode &

HDFS has started the NameNode and DataNode and can begin tasks.
Install sbt:

  echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | tee /etc/apt/sources.list.d/sbt.list
  echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | tee /etc/apt/sources.list.d/sbt_old.list
  curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" |apt-key add
  apt-get update
  apt-get install sbt

Install spark-sql-perf:

  git clone https://github.com/databricks/spark-sql-perf.git
  cd spark-sql-perf
  sbt package

Run Spark:
  
  export SPARK_HOME=/opt/spark-3.4.0-force
  export PATH=$SPARK_HOME/bin:$PATH
  
  spark-shell \
    --conf spark.sql.catalogImplementation=hive \
    --jars /tpcds/spark-sql-perf/target/scala-2.12/spark-sql-perf_2.12-0.5.1-SNAPSHOT.jar
  OR：
  spark-submit \
    --class TPCDSPlanCollector \
    --jars /tpcds/spark-sql-perf/target/scala-2.12/spark-sql-perf_2.12-0.5.1-SNAPSHOT.jar
    TPCDSPlanCollector.scala q64-v2.4 /train/q64.csv

Generate data:
  
  import com.databricks.spark.sql.perf.tpcds.TPCDSTables
  
  val sqlContext = spark.sqlContext
  val scaleFactor = "10"  // 100GB
  val dsdgenDir = "/tpcds/tpcds-kit/tools"
  val dataLocation = "/tpcds/data"
  val dbName = "tpcds"
  val format = "ORC"   
  
  val tables = new TPCDSTables(
    sqlContext,
    dsdgenDir = dsdgenDir,
    scaleFactor = scaleFactor,
    useDoubleForDecimal = false,
    useStringForDate = false
  )
  tables.genData(
    location = dataLocation,
    format = format,
    overwrite = true,
    partitionTables = true,
    clusterByPartitionColumns = false,
    filterOutNullPartitionValues = false,
    tableFilter = "",  
    numPartitions = 10  
  )

Create an external table and import data:

  import com.databricks.spark.sql.perf.tpcds.TPCDSTables
  val sqlContext = spark.sqlContext
  val tables = new TPCDSTables( sqlContext, dsdgenDir = "/tpcds/tpcds-kit/tools", scaleFactor = "10", useDoubleForDecimal = false, useStringForDate = false)
  val dataLocation = "/tpcds/data"
  val format = "ORC"   
  val dbName = "tpcds_new"
  sql(s"CREATE DATABASE IF NOT EXISTS $dbName")
  tables.createExternalTables( location = dataLocation, format = format, databaseName = dbName, overwrite = true, discoverPartitions = true)
  tables.analyzeTables(dbName, analyzeColumns = true)

View databases and tables within them:

  spark.sql("SHOW DATABASES").show(false)
  spark.sql("USE tpcds")
  spark.sql("SHOW TABLES").show(false)
  val count = spark.table("tpcds. catalog_sales ").count()
  println(s"Row count: $count")

Collect execution time (in vanilla Spark):

  import com.databricks.spark.sql.perf.tpcds.TPCDS
  val tpcds = new TPCDS(sqlContext = spark.sqlContext)
  val q64 = tpcds.tpcds2_4Queries.filter(_.name == "q64-v2.4").head
  val sqlText = q64.sqlText.get
  val start = System.nanoTime()
  val df = spark.sql(sqlText)
  df.collect()
  val end = System.nanoTime()
  println(s"Query 64 execution time: ${(end - start) / 1e9} Seconds")

For the source code modification of Spark candidate physical plan to execute all the executions and collect indicators each time:
Modify the file:/spark/sql/core/src/main/scala/org/apache/spark/sql/execution/QueryExecution.scala
Effect of the change: After obtaining a list of candidate physical plans through Catalyst, all plans in the list are executed, and metrics for each execution are recorded.
Stored in the /train folder.
Spark version 3.0.0, Hadoop 2.7, Scala
Compile command: ./build/mvn -DskipTests clean package -Phadoop-2.7 -Phive -Phive-thriftserver
Compiled package location: /spark-3.0.0/sql/core/target/spark-sql_2.12-3.0.0.jar
Copy to the default Spark location on your computer: cp spark-sql_2.12-3.0.0.jar /opt/spark-3.0.0-bin-hadoop2.7/jars
Original compiled file: /Originalsql/spark-sql_2.12-3.0.0.jar
Modified compiled file: /Newsql/spark-sql_2.12-3.0.0.jar

Execution:

  spark.sql("USE tpcds_new")
  import com.databricks.spark.sql.perf.tpcds.TPCDS
  val tpcds = new TPCDS(sqlContext = spark.sqlContext)
  tpcds.tpcds2_4Queries.foreach { q =>
    println(s"===== Executing query: ${q.name} =====")
    val sqlText = q.sqlText.get
    try {
      val df = spark.sql(sqlText)
      df.show(20, false)
    } catch {
      case e: Exception =>
        println(s"[Error] Query ${q.name} failed: ${e.getMessage}")
    }
  }

Cartesian Product Version of Spark 3.0.0 (Collecting Training Data)
Run Spark:

  spark-shell \
    --conf spark.sql.catalogImplementation=hive \
    --jars /tpcds/spark-sql-perf/target/scala-2.12/spark-sql-perf_2.12-0.5.1-SNAPSHOT.jar

Execution:

  import com.databricks.spark.sql.perf.tpcds.TPCDS
  spark.sql("USE tpcds_new")  //Database name
  
  val tpcds   = new TPCDS(sqlContext = spark.sqlContext)
  val queries = tpcds.tpcds2_4Queries   //v2.4 query set (103 items)
  
  // ----File archiving tools----
  import java.nio.file.{Files, Paths, StandardOpenOption, DirectoryStream, Path}
  import java.nio.file.StandardCopyOption.REPLACE_EXISTING
  import java.nio.charset.StandardCharsets
  import java.util.regex.Pattern
  import scala.util.Try
  import scala.util.control.NonFatal
  import scala.jdk.CollectionConverters._
  
  object TrainFileArchiver {
    private val tmpDir   = Paths.get("/tmp")
    private val trainDir = Paths.get("/tmp/train")
  
    private val TMP_JSON = tmpDir.resolve("gnn_training.jsonl")
    private val TMP_CSV  = tmpDir.resolve("gnn_training.csv")
  
    private val IDX_JSON = Pattern.compile("""(\d+)\.jsonl""")
    private val IDX_CSV  = Pattern.compile("""(\d+)\.csv""")
  
    def ensureTrainDir(): Unit = {
      if (Files.notExists(trainDir)) Files.createDirectories(trainDir)
    }
  
    /** Delete /tmp/gnn_training.jsonl / .csv(If exists) */
    def cleanTmpFiles(): Unit = {
      Try(Files.deleteIfExists(TMP_JSON))
      Try(Files.deleteIfExists(TMP_CSV))
    }
  
    /** Returns the integer number of the "next available" directory under /tmp/train (maximum number + 1; 1 if the directory does not exist)*/

    def nextFreeIndex(): Int = {
      ensureTrainDir()
      val ds: DirectoryStream[Path] = Files.newDirectoryStream(trainDir)
      try {
        val used: Set[Int] = ds.asScala.flatMap { p =>
          val n = p.getFileName.toString
          val m1 = IDX_JSON.matcher(n)
          val m2 = IDX_CSV.matcher(n)
          if (m1.matches()) Some(m1.group(1).toInt)
          else if (m2.matches()) Some(m2.group(1).toInt)
          else None
        }.toSet
        if (used.isEmpty) 1 else (used.max + 1)
      } finally ds.close()
    }
  
    /**Move /tmp/gnn_training.* to /tmp/train/{index}.jsonl / {index}.csv (whichever exists) */
    def moveToTrainWithIndex(index: Int): Unit = {
      ensureTrainDir()
      val jsonDst = trainDir.resolve(s"$index.jsonl")
      val csvDst  = trainDir.resolve(s"$index.csv")
  
      var movedAny = false
  
      if (Files.exists(TMP_JSON)) {
        Files.move(TMP_JSON, jsonDst, REPLACE_EXISTING)
        println(s"[move] ${TMP_JSON} -> ${jsonDst}")
        movedAny = true
      } else {
        println(s"[warn] tmp json not found, skip.")
      }
  
      if (Files.exists(TMP_CSV)) {
        Files.move(TMP_CSV, csvDst, REPLACE_EXISTING)
        println(s"[move] ${TMP_CSV} -> ${csvDst}")
        movedAny = true
      } else {
        println(s"[warn] tmp csv not found, skip.")
      }
  
      if (!movedAny) {
        println(s"[warn] nothing moved for index " + index + " (both tmp files missing?)")
      }
    }
  }
  
  //-------Execution------
  def runOne(sqlText: String): Long = {
    val df = spark.sql(sqlText)
    val t0 = System.nanoTime()
    val _ = df.count()                 
    val t1 = System.nanoTime()
    (t1 - t0) / 1000000L
  }
  
  // ---------- 50 Profile combinations-------
  case class Profile(name: String, kv: Map[String,String])
  
  val bool  = Seq("true","false")
  val brThr = Seq("-1","64MB","128MB","256MB")
  val shuf  = Seq("64","128","200","400","800")
  
  val allProfiles: Seq[Profile] = (for {
    aqe <- bool
    cbo <- bool
    smj <- bool                        // spark.sql.join.preferSortMergeJoin
    bt  <- brThr
    sp  <- shuf
  } yield Profile(
    s"aqe=$aqe|cbo=$cbo|smj=$smj|br=$bt|sp=$sp",
    Map(
      "spark.sql.adaptive.enabled" -> aqe,
      "spark.sql.cbo.enabled" -> cbo,
      "spark.sql.cbo.joinReorder.enabled" -> cbo,
      "spark.sql.join.preferSortMergeJoin" -> smj,
      "spark.sql.autoBroadcastJoinThreshold" -> bt,
      "spark.sql.shuffle.partitions" -> sp
    )
  )).take(50)                            
  
  
  // ------------Breakpoint resume main loop --------
  import scala.util.Try
  
  val startIndex = TrainFileArchiver.nextFreeIndex()
  println(s"\n[resume] detected next free index in /tmp/train = $startIndex (will skip earlier ones)\n")
  
  var globalIdx = 0  //Global sequence number: Incremental from 1 in the order of profile × query
  
  var pIdx = 0
  allProfiles.foreach { prof =>
    pIdx += 1
    //Apply the configuration of this profile

    prof.kv.foreach{ case (k,v) => spark.conf.set(k, v) }
    println(s"\n[profile $pIdx/${allProfiles.size}] ${prof.name}")
  
    var qIdx = 0
    queries.foreach { q =>
      qIdx += 1
      globalIdx += 1
  
      if (globalIdx < startIndex) {
        if (globalIdx % 100 == 0) println(s"[skip] global=$globalIdx (already done)")
      } else {
        //Before executing each query: delete /tmp/gnn_training.jsonl / .csv
        TrainFileArchiver.cleanTmpFiles()
  
        //Run the query (your source code will append samples to /tmp/gnn_training.*)
        val name = q.name
        val sql  = q.sqlText.get
        val ms = Try(runOne(sql)).getOrElse(-1L)
        println(f"[ok] global=$globalIdx%05d p=$pIdx%02d q=$qIdx%03d $name runtime=${ms}ms")
  
        //After running: move /tmp/gnn_training. to /tmp/train/{globalIdx}.
        Try(TrainFileArchiver.moveToTrainWithIndex(globalIdx)) recover {
          case NonFatal(e) => println(s"[warn] move failed @global=$globalIdx: ${e.getMessage}")
        }
      }
    }
  }
  println("\n[done] resume-run finished. New files appended to /tmp/train/.")

