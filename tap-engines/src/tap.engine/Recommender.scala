package tap.engine

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLUtils
import scala.util.Try
import spark.jobserver._

class Recommender extends SparkJob with NamedRddSupport {

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    Try(config.getString("Recommender.input0"))
      .map(x => SparkJobValid)
      .getOrElse(SparkJobInvalid("No Recommender.input0 config param"))
  }

  override def runJob(sc: SparkContext, config: Config): Any = {
    // Load and parse the data
    val method = config.getString("Recommender.method")
    val rank = config.getInt("Recommender.rank")
    val numIterations = config.getInt("Recommender.numIterations")
    val input0Name = config.getString("Recommender.input0")
    val ratings = namedRdds.get[org.apache.spark.mllib.recommendation.Rating](input0Name).get

    val model = ALS.train(ratings, rank, numIterations, 0.01)

    // Evaluate the model on rating data
    val usersProducts = ratings.map { case Rating(user, product, rate) =>
      (user, product)
    }
    val predictions = 
      model.predict(usersProducts).map { case Rating(user, product, rate) => 
	((user, product), rate)
      }
    val ratesAndPreds = ratings.map { case Rating(user, product, rate) => 
      ((user, product), rate)
    }.join(predictions)
    val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) => 
      val err = (r1 - r2)
      err * err
    }.mean()

    val result = Map(
      "input0" -> input0Name,
      "numIterations" -> numIterations,
      "rank" -> rank,
      "MSE" -> MSE
    )
    return result
  }
}
