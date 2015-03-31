package tap.engine

import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg.{Vector => SV}
import org.apache.spark.rdd.RDD
import spark.jobserver._

import scala.util.Try

/**
 * Scale Spark mllib vectors using the standard normalization scaler.
 *
 * ==Input Json template==
 *
 * {{{
 * config = """
 * PowerTransform {
 *    input0="inputRddName"
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
 *   input0="inputRddName"
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
object Scale extends SparkJob with NamedRddSupport {

  private val ObjectName = this.getClass.getSimpleName.split('$').head
  private val ConfigInputKey = ObjectName + ".input0"
  private val ConfigOutputKey = ObjectName + ".output0"

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    Try(config.getString(ConfigInputKey))
      .map(x => SparkJobValid)
      .getOrElse(SparkJobInvalid("No " + ConfigInputKey + " config param"))
  }

  override def runJob(sc: SparkContext, config: Config): Any = {
    val input0Name = config.getString(ConfigInputKey)
    val inputVectors = namedRdds.get[SV](input0Name).get
    val outputVectors = normalizeScale(inputVectors)

    val output0Name = config.getString(ConfigOutputKey)
    namedRdds.update(output0Name, outputVectors)

    val result = Map(
      "input0" -> input0Name,
      "output0" -> output0Name
    )
    result
  }

  def normalizeScale(vectors: RDD[SV]): RDD[SV] = {
    val standardScaler = new StandardScaler(withStd = true, withMean = true).fit(vectors)
    vectors.map(v => standardScaler.transform(v)).cache()
  }
}
