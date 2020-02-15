val scalaTestVersion  = "3.1.0"
val antlrVersion      = "4.7.2"
val catsVersion       = "2.1.0"
val scalaCheckVersion = "1.14.3"
val pPrintVersion     = "0.5.8"
val disciplineVersion = "1.0.0"

lazy val root = (project in file("."))
  .enablePlugins(Antlr4Plugin)
  .settings(
    inThisBuild(
      List(
        organization := "ca.valencik",
        scalaVersion := "2.13.1",
        version := "0.1.0-SNAPSHOT"
      )
    ),
    name := "Sequoia",
    libraryDependencies ++= Seq(
      "org.antlr"      % "antlr4-runtime"        % antlrVersion,
      "org.typelevel"  %% "cats-core"            % catsVersion,
      "com.lihaoyi"    %% "pprint"               % pPrintVersion,
      "org.typelevel"  %% "cats-laws"            % catsVersion % Test,
      "org.typelevel"  %% "discipline-scalatest" % disciplineVersion % Test,
      "org.scalatest"  %% "scalatest"            % scalaTestVersion % Test,
      "org.scalacheck" %% "scalacheck"           % scalaCheckVersion % Test
    ),
    antlr4GenListener in Antlr4 := false,
    antlr4GenVisitor in Antlr4 := true,
    antlr4PackageName in Antlr4 := Some("ca.valencik.sequoia"),
    addCompilerPlugin("org.typelevel" %% "kind-projector" % "0.11.0" cross CrossVersion.full),
    scalacOptions in (Compile, console) --= Seq("-Ywarn-unused:imports", "-Xfatal-warnings"),
    scalacOptions in (Test) --= Seq("-Ywarn-unused:imports")
  )
