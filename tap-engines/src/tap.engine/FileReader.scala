package tap.engine

import com.typesafe.config.Config
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.recommendation.Rating
import scala.util.Try
import spark.jobserver._

class FileReader extends SparkJob with NamedRddSupport {

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    Try(config.getString("FileReader.inputFile"))
      .map(x => SparkJobValid)
      .getOrElse(SparkJobInvalid("No FileReader.inputFile config param"))
    Try(config.getString("FileReader.output0"))
      .map(x => SparkJobValid)
      .getOrElse(SparkJobInvalid("No FileReader.output0 config param"))
  }

  override def runJob(sc: SparkContext, config: Config): Any = {
    // Load and parse the data
    val format = config.getString("FileReader.format")
    val data = sc.textFile(config.getString("FileReader.inputFile"))
    val output0Name = config.getString("FileReader.output0")

    namedRdds.update(output0Name, format match {
	case "libSVM" => MLUtils.loadLibSVMFile(sc, config.getString("FileReader.inputFile"))
	case "rating" => sc.textFile(config.getString("FileReader.inputFile")).
	  map(_.split(',') match {case Array(user, item, rate) => Rating(user.toInt, item.toInt, rate.toDouble)})
	case _ => sc.textFile(config.getString("FileReader.inputFile")).
	  map(s => Vectors.dense(s.split(' ').map(_.toDouble)))
    })

    val result = Map(
      "inputFile" -> config.getString("FileReader.inputFile"),
      "output0" -> output0Name
    )
    return result
  }
}
