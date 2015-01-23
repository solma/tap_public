import sbt._
import Keys._

object Dependencies {
  val excludeCglib = ExclusionRule(organization = "org.sonatype.sisu.inject")
  val excludeJackson = ExclusionRule(organization = "org.codehaus.jackson")
  val excludeNetty = ExclusionRule(organization = "org.jboss.netty")
  val excludeAsm = ExclusionRule(organization = "asm")

  lazy val typeSafeConfigDeps = "com.typesafe" % "config" % "1.0.0"
  lazy val yammerDeps = "com.yammer.metrics" % "metrics-core" % "2.2.0"

  lazy val sparkDeps = Seq(
    "org.apache.spark" %% "spark-core" % "1.1.1" % "provided" exclude("io.netty", "netty-all"),
    "org.apache.spark" %% "spark-mllib" % "1.1.1",
    "org.apache.spark" % "spark-streaming_2.10" % "1.1.1",
    "org.apache.spark" % "spark-streaming-twitter_2.10" % "1.1.1",
    "io.spray" %% "spray-json" % "1.2.5",
    "spark.jobserver" % "job-server-api" % "0.4.1" % "provided",
    "org.twitter4j" % "twitter4j-core" % "4.0.2",
    "org.twitter4j" % "twitter4j-stream" % "4.0.2",
    "org.jpmml" % "pmml-evaluator" % "1.1.13",
    // Force netty version.  This avoids some Spark netty dependency problem.
    "io.netty" % "netty" % "3.6.6.Final"
  )

  lazy val slickDeps = Seq(
    "com.typesafe.slick" %% "slick" % "2.0.2-RC1",
    "com.h2database" % "h2" % "1.3.170"
  )

  lazy val logbackDeps = Seq(
    "ch.qos.logback" % "logback-classic" % "1.0.7"
  )

  lazy val coreTestDeps = Seq(
    "org.scalatest" %% "scalatest" % "1.9.1" % "test",
    "com.typesafe.akka" %% "akka-testkit" % "2.2.4" % "test",
    "io.spray" % "spray-testkit" % "1.2.0" % "test"
  )


  lazy val apiDeps = sparkDeps :+ typeSafeConfigDeps

  val repos = Seq(
    "Job Server Bintray" at "https://dl.bintray.com/spark-jobserver/maven",
    "sonatype snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/",
    "spray repo" at "http://repo.spray.io",
    "Typesafe Repo" at "http://repo.typesafe.com/typesafe/releases/"
  )
}
