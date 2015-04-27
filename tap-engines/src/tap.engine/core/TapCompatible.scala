package tap.engine.core

import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import spark.jobserver._

import scala.util.Try
import TapConfig._

/**
 * Trait that abstracts common behaviors of TAP modules. All TAP modules need to implement this trait.
 */
trait TapCompatible {

  val DefaultInputRddKey = "input0"
  val DefaultOutputRddKey = "output0"
  val MockRddPrefix = "__"

  /** All input RDDs' key. */
  val inputRddsKey: Seq[String]
  /** Module name. */
  val moduleName = getClass.getSimpleName.split('$').head
  /** Required module input parameters (excluding input and output Rdd names). */
  val requiredInputConfigKeys: Seq[String]
  /** Required module output result keys. */
  val requiredOutputResultKeys: Seq[String]
  /** Optional module input parameters. */
  val optionalInputConfigKeys: Seq[String] = Nil
  /** Optional module output result keys. */
  val optionalOutputResultKeys: Seq[String] = Nil
  /** All output RDDs' key. */
  val outputRddsKey: Seq[String]

  /**
   * Check if all required parameters are included in the config.
   * @param config: sparkJob config
   * @return
   */
  def checkRequiredInputConfigKeys(config: Config): SparkJobValidation = {
    val missingParams = (inputRddsKey ++ outputRddsKey ++ requiredInputConfigKeys).map(
      para => Try(config.getString(withModuleNamePrefix(para))).map(x => None).getOrElse(Some(para))).flatten
    if (missingParams.isEmpty) SparkJobValid else SparkJobInvalid("Missing required params: " + missingParams.mkString(","))
  }

  def dryRun(namedRdd: NamedRdds, sc: SparkContext, config: Config): RDD[Any] = {
    // if no need of upstream RDD
    if (inputRddsKey == Nil) {
      updateMockOutputRdd(generateMockData(Map()), sc, config)
    }
    else {
      val upstreamMockedRdds = inputRddsKey.map(rddKey =>
        rddKey -> {
          val mockedRdd = namedRdd.get[Any](MockRddPrefix + config.getString(withModuleNamePrefix(rddKey)))
          if (mockedRdd == None) None else mockedRdd.get
        }).toMap
      // any upstream mocked RDD is not available
      val rdds = Seq(upstreamMockedRdds.values)
      if (rdds.flatten.size < rdds.size) {
        val upstreamRdds = inputRddsKey.map(rddKey =>
          rddKey -> {
            val rdd = namedRdd.get[Any](config.getString(withModuleNamePrefix(rddKey)))
            if (rdd == None) None else rdd.get
          }).toMap
        // any upstream RDD is not available
        val rdds = Seq(upstreamRdds.values)
        if (rdds.flatten.size < rdds.size) {
          throw new NoSuchElementException(
            "for some upstream RDD neither itself nor its mock data is available.")
        } else {
          updateMockOutputRdd(generateMockData(upstreamRdds), sc, config)
        }
      } else {
        updateMockOutputRdd(generateMockData(upstreamMockedRdds), sc, config)
      }
    }
  }

  /**
   * Generates mock data.
   * @param upstreamInputMap: input data map (Name: Data) from upstream RDDs or mocked RDDs
   * @return mocked output
   */
  def generateMockData(upstreamInputMap: Map[String, Any]): Seq[Any]

  def run(namedRdd: NamedRdds, sc: SparkContext, config: Config): Any =
    if (isDryRun()) dryRun(namedRdds, sc, config) else trueRun(sc, config)

  def trueRun(sc: SparkContext, config: Config): Any

  def updateMockOutputRdd(mocked: Seq[Any], sc: SparkContext, config: Config): RDD[Any] =
    namedRdds.update(MockRddPrefix + config.getString(withModuleNamePrefix(DefaultOutputRddKey)), sc.parallelize(mocked))

  // get paraName with moduleName prefix
  def withModuleNamePrefix(paraName: String): String =
    if (paraName.startsWith(moduleName)) paraName else moduleName + '.' + paraName

  // get paraName without moduleName prefix
  def withoutModuleNamePrefix(paraName: String): String =
    if (paraName.startsWith(moduleName)) paraName.split(".").tail.mkString(".") else paraName
}
