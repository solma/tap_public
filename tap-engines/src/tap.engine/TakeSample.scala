package tap.engine

import com.typesafe.config.Config
import org.apache.spark._
import org.apache.spark.mllib.linalg.{Vector => SV}
import org.apache.spark.mllib.recommendation.Rating
import spark.jobserver._

object TakeSample extends SparkJob with NamedRddSupport with TapCompatible {

  val inputRddsKey = Seq(DefaultInputRddKey)
  val outputRddsKey = Nil

  val iCount = "count"
  val iSeed = "seed"
  override val requiredInputConfigKeys = Seq(iCount, iSeed)

  val oData = "data"
  override val requiredOutputResultKeys = Seq(oData)

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    checkRequiredInputConfigKeys(config)
  }

  override def runJob(sc: SparkContext, config: Config): Any = {
   val result = Map(
      DefaultInputRddKey -> config.getString(withModuleNamePrefix(DefaultInputRddKey)),
      iCount -> config.getInt(withModuleNamePrefix(iCount)),
      oData -> run(namedRdds, sc, config)
    )
    result
  }

  override def generateMockData(upstreamInputMap: Map[String, Any]): Seq[Any] = {
    val RatingClassName = Rating.getClass.getName
    upstreamInputMap.get(DefaultInputRddKey).getClass.getName match {
      case RatingClassName => {
        val rating = upstreamInputMap.asInstanceOf[Rating]
        Seq(rating.user, rating.product, rating.rating)
      }
      case _ => Seq(upstreamInputMap)
    }
  }

  override def trueRun(sc: SparkContext, config: Config): Any = {
    val seed = config.getLong(withModuleNamePrefix(iSeed))
    val inputRDD = namedRdds.get(config.getString(withModuleNamePrefix(DefaultInputRddKey))).get
    inputRDD.takeSample(false, config.getInt(withModuleNamePrefix(iCount)), seed)
  }
}
