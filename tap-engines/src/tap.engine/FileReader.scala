package tap.engine

import com.typesafe.config.Config
import org.apache.spark._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.mllib.util.MLUtils
import spark.jobserver._

import scala.util.Try

/**
 * Load input data from storage sources, such as Google CloudStorage.
 *
 * ==Input Json template==
 *
 * {{{
 * config = """
 * FileReader {
 *    inputFile="gs://yourBucket/yourFile"
 *    format="format (one of the following: CSV)"
 *    output0="outputRddName"
 * }
 * """
 * }}}
 *
 * To construct your own config, simply replace the values in the quotes above.
 *
 * ==Output Json format==
 * {{{
 * result =
 * {
 *   inputFile="gs://yourBucket/yourFile"
 *   output0="outputRddName"
 * }
 * }}}
 *
 * To retrieve a value by key, simply use the key name.
 *
 * E.g. the following code retrieves the name of the output RDD
 * (assuming `resp` is the object name returned from the call to this module).
 * {{{
 *   resp['result']['output0']
 * }}}
 */

object FileReader extends SparkJob with NamedRddSupport {

  val ObjectName = this.getClass.getSimpleName.split('$').head

  val InputFileKeyName = "inputFile"
  val InputFileKeyPath = ObjectName + "." + InputFileKeyName
  val InputFileFormatKeyPath = ObjectName + ".format"
  val OutputRddKeyName = "output0"
  val OutputRddNameKeyPath = ObjectName + "." + OutputRddKeyName

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    Try(config.getString(InputFileKeyPath))
      .map(x => SparkJobValid)
      .getOrElse(SparkJobInvalid("No" + InputFileKeyPath + "config param"))
    Try(config.getString(OutputRddNameKeyPath))
      .map(x => SparkJobValid)
      .getOrElse(SparkJobInvalid("No" + OutputRddNameKeyPath + "config param"))
  }

  /**
   * @param sc spark context
   * @param config json object provides input parameters
   * @return json object provides output parameters
   */
  override def runJob(sc: SparkContext, config: Config): Any = {
    // Load and parse the data
    val format = config.getString(InputFileFormatKeyPath)
    val inputFilePath = config.getString(InputFileKeyPath)
    val output0Name = config.getString(OutputRddNameKeyPath)

    namedRdds.update(output0Name, format match {
      case "libSVM" => MLUtils.loadLibSVMFile(sc, inputFilePath)
      case "rating" => sc.textFile(inputFilePath).
        map(_.split(',') match {
          case Array(user, item, rate) => Rating(user.toInt, item.toInt, rate.toDouble)
        })
      case _ => sc.textFile(inputFilePath).
        map(s => Vectors.dense(s.split(' ').map(_.toDouble)))
    })

    val result = Map(
      InputFileKeyName -> inputFilePath,
      OutputRddKeyName -> output0Name
    )
    result
  }
}
