package tap.engine

import com.typesafe.config.Config
import org.apache.spark._
import org.apache.spark.mllib.linalg.{Vector => SV}
import org.apache.spark.mllib.recommendation.Rating
import spark.jobserver._
import tap.engine.TapConfig._
import tap.engine.TapUtil._

import scala.util.Try

object TakeSample extends SparkJob with NamedRddSupport {

  val ObjectName = this.getClass.getSimpleName.split('$').head

  val InputRddKeyPath = ObjectName + '.' + InputRddKey
  val OutputRddKeyPath = ObjectName + '.' + OutputRddKey

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    Try(config.getString(InputRddKeyPath))
      .map(x => SparkJobValid)
      .getOrElse(SparkJobInvalid("No " + InputRddKeyPath + "config param"))
    Try(config.getString("TakeSample.count"))
      .map(x => SparkJobValid)
      .getOrElse(SparkJobInvalid("No TakeSample.count config param"))
  }

  override def runJob(sc: SparkContext, config: Config): Any = {
    val input0Name = config.getString(InputRddKeyPath)
    val count = config.getInt("TakeSample.count")
    val result = Map(
      InputRddKey -> input0Name,
      "count" -> count
    )

    if (isDryRun()) {
      val firstElement = namedRdds.get[Any](DryRunRddPrefix + config.getString(InputRddKeyPath)).get.first()
      val RatingClassName = Rating.getClass.getName
      val mockData = namedRdds.update(DryRunRddPrefix + config.getString(OutputRddKeyPath),
        sc.parallelize(firstElement.getClass.getName match {
          case RatingClassName => {
            val rating = firstElement.asInstanceOf[Rating]
            Seq(rating.user, rating.product, rating.rating)
          }
          case _ => Seq(firstElement)
        }))
      result + ("data" -> mockData)
    } else {
      val seed = config.getLong(ObjectName + ".seed")
      val inputRDD = namedRdds.get(input0Name).get
      result + ("data" -> inputRDD.takeSample(false, count, seed))
    }
  }
}
