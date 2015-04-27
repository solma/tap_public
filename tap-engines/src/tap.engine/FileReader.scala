package tap.engine

import com.typesafe.config.Config
import org.apache.spark._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.mllib.util.MLUtils
import spark.jobserver._

/**
 * Load input data from storage sources, such as Google CloudStorage.
 *
 * ==Input Json template==
 *
 * {{{
 * config = """
 * FileReader {
 *    inputFile="gs://yourBucket/yourFile"
 *    format="CSV" # options include "rating", "libSVM"
 *    delimiter="," # not required if the format (e.g. "libSVM") implicitly defines a delimiter
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

object FileReader extends SparkJob with TapCompatible with NamedRddSupport {

  val inputRddsKey = Nil
  val outputRddsKey = Seq(DefaultOutputRddKey)

  val iFormat = "format"
  val iInputFile = "inputFile"
  override val requiredInputConfigKeys = Seq(iFormat, iInputFile)

  val iDelimiter = "delimiter"
  override val optionalInputConfigKeys = Seq(iDelimiter)

  override val requiredOutputResultKeys = Seq(DefaultInputRddKey, DefaultOutputRddKey)

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    checkRequiredInputConfigKeys(config)
  }

  /**
   * @param sc spark context
   * @param config json object provides input parameters
   * @return json object provides output parameters
   */
  override def runJob(sc: SparkContext, config: Config): Any = {
    run(namedRdds, sc, config)

    val result = Map(
      DefaultInputRddKey -> config.getString(withModuleNamePrefix(iInputFile)),
      DefaultOutputRddKey -> config.getString(withModuleNamePrefix(DefaultOutputRddKey))
    )
    result
  }

  override def generateMockData(upstreamInputMap: Map[String, Any]): Seq[Any] =
    Seq(Vectors.dense(1, 2, 3), Vectors.dense(4, 5, 6), Vectors.dense(7, 8, 9))

  override def trueRun(sc: SparkContext, config: Config): Any = {
    val delimiter = config.getString(withModuleNamePrefix(iDelimiter))
    val format = config.getString(withModuleNamePrefix(iFormat))
    val inputFilePath = config.getString(withModuleNamePrefix(iInputFile))
    val output0Name = config.getString(withModuleNamePrefix(DefaultOutputRddKey))
    namedRdds.update(output0Name, format match {
      case "libSVM" => MLUtils.loadLibSVMFile(sc, inputFilePath)
      case "rating" => sc.textFile(inputFilePath).
        map(_.split(',') match {
        case Array(user, item, rate) => Rating(user.toInt, item.toInt, rate.toDouble)
      })
      case _ => sc.textFile(inputFilePath).
        map(s => Vectors.dense(s.split(delimiter).map(_.toDouble)))
    })
  }
}
