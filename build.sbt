lazy val ScalaTestVersion  = "3.0.5"
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
      "com.facebook.presto" % "presto-parser" % prestoVersion,
      "org.antlr" % "antlr4-runtime" % "4.6"
    )
  )
