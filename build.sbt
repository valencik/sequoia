// https://typelevel.org/sbt-typelevel/faq.html#what-is-a-base-version-anyway
ThisBuild / tlBaseVersion := "0.0" // your current series x.y

ThisBuild / organization     := "io.pig"
ThisBuild / organizationName := "Pig.io"
ThisBuild / startYear        := Some(2022)
ThisBuild / licenses         := Seq(License.Apache2)
ThisBuild / developers := List(
  // your GitHub handle and name
  tlGitHubDev("valencik", "Andrew Valencik")
)

// publish to s01.oss.sonatype.org (set to true to publish to oss.sonatype.org instead)
ThisBuild / tlSonatypeUseLegacyHost := false

// publish website from this branch
ThisBuild / tlSitePublishBranch := Some("main")

// publish snapshots from main branch
ThisBuild / tlCiReleaseBranches := Seq("main")

// use JDK 11
ThisBuild / githubWorkflowJavaVersions := Seq(JavaSpec.temurin("11"))

val Scala213 = "2.13.8"
ThisBuild / crossScalaVersions := Seq(Scala213)
ThisBuild / scalaVersion       := Scala213 // the default Scala

val scalaTestVersion  = "3.2.13"
val antlrVersion      = "4.11.1"
val catsVersion       = "2.8.0"
val scalaCheckVersion = "1.17.0"
val pPrintVersion     = "0.7.3"
val disciplineVersion = "2.2.0"
val paigesVersion     = "0.4.2"
val monocleVersion    = "3.1.0"

lazy val root = tlCrossRootProject
  .aggregate(core, pretty, rewrite, parse, examples, tests, laws)

lazy val core = project
  .in(file("modules/core"))
  .settings(
    name := "sequoia-core",
    libraryDependencies ++= Seq(
      "org.typelevel" %% "cats-core" % catsVersion
    )
  )

lazy val pretty = project
  .in(file("modules/pretty"))
  .dependsOn(core)
  .settings(
    name := "sequoia-pretty",
    libraryDependencies ++= Seq(
      "org.typelevel" %% "cats-core"   % catsVersion,
      "org.typelevel" %% "paiges-core" % paigesVersion
    )
  )

lazy val rewrite = project
  .in(file("modules/rewrite"))
  .dependsOn(core)
  .settings(
    name := "sequoia-rewrite",
    libraryDependencies ++= Seq(
      "org.typelevel" %% "cats-core"    % catsVersion,
      "dev.optics"    %% "monocle-core" % monocleVersion,
      "com.lihaoyi"   %% "pprint"       % pPrintVersion
    )
  )

lazy val parse = project
  .in(file("modules/parse"))
  .dependsOn(core)
  .enablePlugins(Antlr4Plugin)
  .settings(
    name := "sequoia-parse",
    libraryDependencies ++= Seq(
      "org.antlr"    % "antlr4-runtime" % antlrVersion,
      "com.lihaoyi" %% "pprint"         % pPrintVersion
    ),
    Antlr4 / antlr4Version     := antlrVersion,
    Antlr4 / antlr4GenListener := false,
    Antlr4 / antlr4GenVisitor  := true,
    Antlr4 / antlr4PackageName := Some("io.pig.sequoia")
  )

lazy val examples = project
  .in(file("examples"))
  .enablePlugins(NoPublishPlugin)
  .dependsOn(core, pretty, rewrite, parse)

lazy val tests = project
  .in(file("modules/tests"))
  .enablePlugins(NoPublishPlugin)
  .dependsOn(core, parse)
  .settings(
    libraryDependencies ++= Seq(
      "org.antlr"      % "antlr4-runtime" % antlrVersion,
      "org.typelevel" %% "cats-core"      % catsVersion,
      "org.scalatest" %% "scalatest"      % scalaTestVersion % Test
    )
  )

lazy val laws = project
  .in(file("modules/laws"))
  .enablePlugins(NoPublishPlugin)
  .dependsOn(core, parse)
  .settings(
    libraryDependencies ++= Seq(
      "org.typelevel"  %% "cats-core"            % catsVersion,
      "org.typelevel"  %% "cats-laws"            % catsVersion       % Test,
      "org.typelevel"  %% "discipline-scalatest" % disciplineVersion % Test,
      "org.scalatest"  %% "scalatest"            % scalaTestVersion  % Test,
      "org.scalacheck" %% "scalacheck"           % scalaCheckVersion % Test
    )
  )
