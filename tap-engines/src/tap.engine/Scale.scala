package tap.engine

import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg.{Vector => SV}
import org.apache.spark.rdd.RDD
import spark.jobserver._

import scala.util.Try

object Scale extends SparkJob with NamedRddSupport {

  private val configInputKey = "Scale.input0"
  private val configOutputKey = "Scale.output0"

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    Try(config.getString(configInputKey))
      .map(x => SparkJobValid)
      .getOrElse(SparkJobInvalid("No " + configInputKey + " config param"))
  }

  override def runJob(sc: SparkContext, config: Config): Any = {
    val input0Name = config.getString(configInputKey)
    val inputVectors = namedRdds.get[SV](input0Name).get
    val outputVectors = normalizeScale(inputVectors)

    val output0Name = config.getString(configOutputKey)
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
