package tap.engine

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.linalg.Vectors
import scala.util.Try
import spark.jobserver._

object TakeSample extends SparkJob with NamedRddSupport {

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    Try(config.getString("TakeSample.input0"))
      .map(x => SparkJobValid)
      .getOrElse(SparkJobInvalid("No TakeSample.input0 config param"))
    Try(config.getString("TakeSample.count"))
      .map(x => SparkJobValid)
      .getOrElse(SparkJobInvalid("No TakeSample.count config param"))
  }

  override def runJob(sc: SparkContext, config: Config): Any = {
    val input0Name = config.getString("TakeSample.input0")
    val count = config.getInt("TakeSample.count")
    val seed = config.getLong("TakeSample.seed")
    // val inputRDD = namedRdds.get(input0Name).get
    val inputRDD = namedRdds.get[org.apache.spark.mllib.linalg.Vector](input0Name).get

    // val sampleOutput = inputRDD.takeSample(false, count, seed)
    // val sampleOutput = inputRDD.take(count)

    val result = Map(
      "input0" -> input0Name,
      "count" -> count,
      "data" -> inputRDD.takeSample(false, count, seed)
    )
    return result
  }
}
