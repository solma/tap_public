package tap.engine

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLUtils
import scala.util.Try
import spark.jobserver._

class Evaluator extends SparkJob with NamedRddSupport {

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    Try(config.getString("Evaluator.pmmlString"))
      .map(x => SparkJobValid)
      .getOrElse(SparkJobInvalid("No Evaluator.input0 config param"))
  }

  override def runJob(sc: SparkContext, config: Config): Any = {
    // Load and parse the data
    val pmmlString = config.getString("Evaluator.pmmlString")

    new JPMMLEvaluator(pmmlString)

    val result = Map(
      "pmmlString" -> pmmlString
    )
    return result
  }
}
