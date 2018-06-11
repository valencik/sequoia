lazy val ScalaTestVersion  = "3.0.5"
lazy val sparkVersion = "2.3.0"
lazy val prestoVersion = "0.203"

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
      "org.apache.spark" %% "spark-core"     % sparkVersion,
      "org.apache.spark" %% "spark-sql"      % sparkVersion,
      "com.facebook.presto" % "presto-parser" % prestoVersion,
      "org.antlr" % "antlr4-runtime" % "4.6"
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

assemblyMergeStrategy in assembly := {
  case "git.properties" => MergeStrategy.discard
  case PathList(ps @ _*) if ps.last endsWith "UnusedStubClass.class" => MergeStrategy.first
  case x => (assemblyMergeStrategy in assembly).value(x)
}
assemblyShadeRules in assembly ++= Seq(
  ShadeRule.rename("javax.inject.**" -> "shade.@0").inLibrary("javax.inject" % "javax.inject" % "1"),
  ShadeRule.rename("org.antlr.**" -> "shade.@0").inLibrary("org.antlr" % "antlr4-runtime" % "4.6"),
  ShadeRule.rename("org.aopalliance.**" -> "shade.@0").inLibrary("org.glassfish.hk2.external" % "aopalliance-repackaged" % "2.4.0-b34"),
  ShadeRule.rename("org.apache.commons.**" -> "shade.@0").inLibrary("commons-beanutils" % "commons-beanutils" % "1.7.0"),
  ShadeRule.rename("org.apache.commons.**" -> "shade2.@0").inLibrary("commons-collections" % "commons-collections" % "3.2.2"),
  ShadeRule.rename("org.apache.hadoop.**" -> "shade.@0").inLibrary("org.apache.hadoop" % "hadoop-yarn-common" % "2.6.5"),
)
