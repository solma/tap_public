package tap.engine

import com.typesafe.config.Config
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vectors
import scala.util.Try
import spark.jobserver._

class FileWriter extends SparkJob with NamedRddSupport {

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    Try(config.getString("FileWriter.input0"))
      .map(x => SparkJobValid)
      .getOrElse(SparkJobInvalid("No FileWriter.input0 config param"))
    Try(config.getString("FileWriter.outputFile"))
      .map(x => SparkJobValid)
      .getOrElse(SparkJobInvalid("No FileWriter.outputFile config param"))
  }

  override def runJob(sc: SparkContext, config: Config): Any = {
    // Load and parse the data
    val input0Name = config.getString("FileWriter.input0")
    val outputFileName = config.getString("FileWriter.outputFile")

    val data = namedRdds.get[org.apache.spark.mllib.linalg.Vector](input0Name).get
    data.saveAsTextFile(outputFileName)

    val result = Map(
      "input0" -> input0Name,
      "outputFile" -> outputFileName
    )
    return result
  }
}
