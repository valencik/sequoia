ThisBuild / scalaVersion := "2.13.2"

val scalaTestVersion  = "3.1.1"
val antlrVersion      = "4.7.2"
val catsVersion       = "2.1.1"
val scalaCheckVersion = "1.14.3"
val pPrintVersion     = "0.5.9"
val disciplineVersion = "1.0.1"

lazy val commonSettings = Seq(
  organization := "ca.valencik",
  version := "0.1.0-SNAPSHOT",
  addCompilerPlugin("org.typelevel" %% "kind-projector" % "0.11.0" cross CrossVersion.full)
)

lazy val root = (project in file("."))
  .aggregate(core, parse, tests, laws)

lazy val core = (project in file("modules/core"))
  .settings(commonSettings)
  .settings(
    name := "sequoia-core",
    libraryDependencies ++= Seq(
      "org.typelevel" %% "cats-core" % catsVersion
    )
  )

lazy val parse = (project in file("modules/parse"))
  .dependsOn(core)
  .enablePlugins(Antlr4Plugin)
  .settings(commonSettings)
  .settings(
    name := "sequoia-parse",
    libraryDependencies ++= Seq(
      "org.antlr"    % "antlr4-runtime" % antlrVersion,
      "com.lihaoyi" %% "pprint"         % pPrintVersion
    ),
    antlr4GenListener in Antlr4 := false,
    antlr4GenVisitor in Antlr4 := true,
    antlr4PackageName in Antlr4 := Some("ca.valencik.sequoia")
  )

lazy val tests = (project in file("modules/tests"))
  .dependsOn(core, parse)
  .settings(commonSettings)
  .settings(
    libraryDependencies ++= Seq(
      "org.antlr"      % "antlr4-runtime" % antlrVersion,
      "org.typelevel" %% "cats-core"      % catsVersion,
      "org.scalatest" %% "scalatest"      % scalaTestVersion % Test
    )
  )

lazy val laws = (project in file("modules/laws"))
  .dependsOn(core, parse)
  .settings(commonSettings)
  .settings(
    libraryDependencies ++= Seq(
      "org.typelevel"  %% "cats-core"            % catsVersion,
      "org.typelevel"  %% "cats-laws"            % catsVersion       % Test,
      "org.typelevel"  %% "discipline-scalatest" % disciplineVersion % Test,
      "org.scalatest"  %% "scalatest"            % scalaTestVersion  % Test,
      "org.scalacheck" %% "scalacheck"           % scalaCheckVersion % Test
    )
  )
