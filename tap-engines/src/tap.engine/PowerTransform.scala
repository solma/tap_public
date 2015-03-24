package tap.engine

import breeze.linalg.DenseVector
import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{Vectors, Vector => SV}
import org.apache.spark.rdd.RDD
import spark.jobserver._
import scala.collection.JavaConversions._
import scala.util.Try

object PowerTransform extends SparkJob with NamedRddSupport {

  private val configInputKey = "PowerTransform.input0"
  private val configParamKey = "PowerTransform.lambda"
  private val configOutputKey = "PowerTransform.output0"

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    Try(config.getString(configInputKey))
      .map(x => SparkJobValid)
      .getOrElse(SparkJobInvalid("No " + configInputKey + " config param"))

    Try(config.getString(configParamKey))
      .map(x => SparkJobValid)
      .getOrElse(SparkJobInvalid("No " + configParamKey + " config param"))
  }

  override def runJob(sc: SparkContext, config: Config): Any = {
    val input0Name = config.getString(configInputKey)
    val inputVectors = namedRdds.get[SV](input0Name).get
    // java double to scala double
    val lambdaVector = config.getDoubleList(configParamKey).toVector.map(_.doubleValue())
    val outputVectors = boxCox(inputVectors, lambdaVector)

    val output0Name = config.getString(configOutputKey)
    namedRdds.update(output0Name, outputVectors)

    val result = Map(
      "input0" -> input0Name,
      "output0" -> output0Name
    )
    result
  }

  def boxCox(vectors: RDD[SV], lambda: Vector[Double]): RDD[SV] = {
    vectors.map(transform(_, lambda)).cache()
  }

  def transform(v: SV, lambda: Vector[Double]): SV = {
    require(v.size == lambda.size, "Vectors must be the same length!")

    val transformed = DenseVector(v.toArray)
    for ((i, x) <- transformed.activeIterator) {
      transformed(i) = math.boxCox(v(i), lambda(i))
    }
    Vectors.dense(transformed.toArray)
  }
}
