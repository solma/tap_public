import sbt._
import Keys._
import sbtassembly.Plugin._
import AssemblyKeys._
import spray.revolver.RevolverPlugin._
import spray.revolver.Actions
import com.typesafe.sbt.SbtScalariform._
import org.scalastyle.sbt.ScalastylePlugin
import scalariform.formatter.preferences._
import bintray.Plugin.bintrayPublishSettings

// There are advantages to using real Scala build files with SBT:
//  - Multi-JVM testing won't work without it, for now
//  - You get full IDE support
object TapBuild extends Build {
  lazy val dirSettings = Seq(
    unmanagedSourceDirectories in Compile <<= Seq(baseDirectory(_ / "src" )).join,
    unmanagedSourceDirectories in Test <<= Seq(baseDirectory(_ / "test" )).join,
    scalaSource in Compile <<= baseDirectory(_ / "src" ),
    scalaSource in Test <<= baseDirectory(_ / "test" )
  )

  import Dependencies._

  lazy val tapEnginesJar = Project(id = "tap-engines", base = file("tap-engines"),
    settings = commonSettings210 ++ Assembly.settings ++ Seq(libraryDependencies ++= sparkDeps ++ apiDeps,
                                        publishArtifact := false,
                                        description := "Test jar for Spark Job Server",
                                        exportJars := true)   // use the jar instead of target/classes
  )

  // To add an extra jar to the classpath when doing "re-start" for quick development, set the
  // env var EXTRA_JAR to the absolute full path to the jar
  lazy val extraJarPaths = Option(System.getenv("EXTRA_JAR"))
                             .map(jarpath => Seq(Attributed.blank(file(jarpath))))
                             .getOrElse(Nil)

  // Create a default Scala style task to run with compiles
  lazy val runScalaStyle = taskKey[Unit]("testScalaStyle")

  lazy val commonSettings210 = Defaults.defaultSettings ++ dirSettings ++ Seq(
    organization := "moto.dp",
    crossPaths   := false,
    scalaVersion := "2.10.4",
    scalaBinaryVersion := "2.10",
    publishTo    := Some(Resolver.file("Unused repo", file("target/unusedrepo"))),

    runScalaStyle := {
      org.scalastyle.sbt.PluginKeys.scalastyle.toTask("").value
    },
    (compile in Compile) <<= (compile in Compile) dependsOn runScalaStyle,

    // In Scala 2.10, certain language features are disabled by default, such as implicit conversions.
    // Need to pass in language options or import scala.language.* to enable them.
    // See SIP-18 (https://docs.google.com/document/d/1nlkvpoIRkx7at1qJEZafJwthZ3GeIklTFhqmXMvTX9Q/edit)
    scalacOptions := Seq("-deprecation", "-feature",
                         "-language:implicitConversions", "-language:postfixOps"),
    resolvers    ++= Dependencies.repos,
    libraryDependencies ++= apiDeps,
    parallelExecution in Test := false,
    // We need to exclude jms/jmxtools/etc because it causes undecipherable SBT errors  :(
    ivyXML :=
      <dependencies>
        <exclude module="jms"/>
        <exclude module="jmxtools"/>
        <exclude module="jmxri"/>
      </dependencies>
  ) ++ scalariformPrefs ++ ScalastylePlugin.Settings ++ scoverageSettings

  lazy val scoverageSettings = {
    import ScoverageSbtPlugin._
    instrumentSettings ++ Seq(
      // Semicolon-separated list of regexs matching classes to exclude
      ScoverageKeys.excludedPackages in scoverage := ".+Benchmark.*"
    )
  }

  lazy val publishSettings = bintrayPublishSettings ++ Seq(
    licenses += ("Apache-2.0", url("http://choosealicense.com/licenses/apache/")),
    bintray.Keys.bintrayOrganization in bintray.Keys.bintray := Some("moto-dp")
  )

  // change to scalariformSettings for auto format on compile; defaultScalariformSettings to disable
  // See https://github.com/mdr/scalariform for formatting options
  lazy val scalariformPrefs = defaultScalariformSettings ++ Seq(
    ScalariformKeys.preferences := FormattingPreferences()
      .setPreference(AlignParameters, true)
      .setPreference(AlignSingleLineCaseStatements, true)
      .setPreference(DoubleIndentClassDeclaration, true)
      .setPreference(PreserveDanglingCloseParenthesis, false)
  )
}
