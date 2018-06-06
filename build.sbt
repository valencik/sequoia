lazy val ScalaTestVersion  = "3.0.5"
lazy val sparkVersion = "2.3.0"

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "ca.valencik",
      scalaVersion := "2.11.12",
      version      := "0.1.0-SNAPSHOT",
      javaOptions in Test ++= Seq("-Xmx1G", "-ea"),
      fork in Test := true,
      parallelExecution in Test := false
    )),
    name := "Big SQL Parse",
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % ScalaTestVersion % Test,
      "org.apache.spark" %% "spark-core"     % sparkVersion % "provided",
      "org.apache.spark" %% "spark-sql"      % sparkVersion % "provided"
    )
  ).settings(repl: _*)

lazy val repl = Seq(
  initialize ~= { _ => // Color REPL
    val ansi = System.getProperty("sbt.log.noformat", "false") != "true"
    if (ansi) System.setProperty("scala.color", "true")
  },
  initialCommands in console :=
    """
      |import org.apache.spark.{SparkConf, SparkContext}
      |import org.apache.spark.sql.SparkSession
      |
      |val conf = new SparkConf().setMaster("local[*]").setAppName("repl").set("spark.ui.enabled", "false")
      |implicit val spark = SparkSession.builder().config(conf).appName("REPL").getOrCreate()
      |
      |import spark.implicits._
      |
      |spark.sparkContext.setLogLevel("WARN")
      |
    """.stripMargin,
  cleanupCommands in console :=
    """
      |spark.stop()
    """.stripMargin
)
