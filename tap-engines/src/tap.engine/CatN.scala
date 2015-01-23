package tap.engine

import com.typesafe.config.Config
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vectors
import scala.util.Try
import spark.jobserver._

class CatN extends SparkJob with NamedRddSupport {

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    Try(config.getString("CatN.input0"))
      .map(x => SparkJobValid)
      .getOrElse(SparkJobInvalid("No CatN.input0 config param"))
  }

  override def runJob(sc: SparkContext, config: Config): Any = {
    // Load and parse the data
    val input0Name = config.getString("CatN.input0")
    val n = config.getInt("CatN.n")

    val data = namedRdds.get[org.apache.spark.mllib.linalg.Vector](input0Name).get

    val result = Map(
      "input0" -> input0Name,
      "n" -> n,
      "data" -> data.take(n)
    )
    return result
  }
}
