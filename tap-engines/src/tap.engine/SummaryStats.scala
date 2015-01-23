package tap.engine

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import scala.util.Try
import spray.json._
import spray.json.DefaultJsonProtocol._
import spark.jobserver._

object SummaryStats extends SparkJob with NamedRddSupport {

  def main(args: Array[String]) {
    val sc = new SparkContext("local[4]", "WordCountExample")
    val config = ConfigFactory.parseString("")
    val results = runJob(sc, config)
    println("Result is " + results)
  }

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    Try(config.getString("SummaryStats.input0"))
      .map(x => SparkJobValid)
      .getOrElse(SparkJobInvalid("No SummaryStats.input0 config param"))
  }

  override def runJob(sc: SparkContext, config: Config): Any = {
    val input0Name = config.getString("SummaryStats.input0")
    val observations = namedRdds.get[org.apache.spark.mllib.linalg.Vector](input0Name).get

    val summary: MultivariateStatisticalSummary = Statistics.colStats(observations)

    val result = Map(
      "input0" -> input0Name,
      "count" -> summary.count,
      "max" -> summary.max,
      "min" -> summary.min,
      "mean" -> summary.mean,
      "numNonzeros" -> summary.numNonzeros,
      "variance" -> summary.variance
    )
    return result
  }
}
