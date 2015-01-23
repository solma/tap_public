package tap.engine

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.random.RandomRDDs._
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import scala.util.Try
import spray.json._
import spray.json.DefaultJsonProtocol._
import spark.jobserver._

object RandomData extends SparkJob with NamedRddSupport {

  def main(args: Array[String]) {
    val sc = new SparkContext("local[4]", "WordCountExample")
    val config = ConfigFactory.parseString("")
    val results = runJob(sc, config)
    println("Result is " + results)
  }

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    Try(config.getString("RandomData.mode"))
      .map(x => SparkJobValid)
      .getOrElse(SparkJobInvalid("No RandomData.mode config param"))
    Try(config.getString("RandomData.output0"))
      .map(x => SparkJobValid)
      .getOrElse(SparkJobInvalid("No RandomData.out0 config param"))
  }

  override def runJob(sc: SparkContext, config: Config): Any = {
    val mode = config.getString("RandomData.mode")
    val output0Name = config.getString("RandomData.output0")
    val numRows = config.getLong("RandomData.numRows")
    val numCols = config.getInt("RandomData.numCols")

    val randomRdd = mode match {
      case "uniformVector" => uniformVectorRDD(sc, numRows, numCols)
      case "normalVector" => normalVectorRDD(sc, numRows, numCols)
    }

    namedRdds.update(output0Name, randomRdd)

    val result = Map(
      "output0" -> output0Name
    )
    return result
  }
}
