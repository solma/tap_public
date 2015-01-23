package tap.engine

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.Vectors
import scala.util.Try
import spray.json._
import spray.json.DefaultJsonProtocol._
import spark.jobserver._

import java.io.{ObjectOutputStream, ObjectInputStream}
import java.io.{FileOutputStream, FileInputStream}

object KMeansPredict extends SparkJob with NamedRddSupport {

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    Try(config.getString("KMeansPredict.input0"))
      .map(x => SparkJobValid)
      .getOrElse(SparkJobInvalid("No KMeansPredict.input0 config param"))
  }

  override def runJob(sc: SparkContext, config: Config): Any = {
    // Load and parse the data
    val input0Name = config.getString("KMeansPredict.input0")
    val output0Name = config.getString("KMeansPredict.output0")
    val vectorData = namedRdds.get[org.apache.spark.mllib.linalg.Vector](input0Name).get
    // val centroidsList = config.getObjectList("KMeansPredict.centroids")

    val is = new ObjectInputStream(new FileInputStream("/tmp/example.dat"))
    val obj = is.readObject()
    is.close()

    val model = obj.asInstanceOf[KMeansModel]
    val indicies = model.predict(vectorData)
    // val mergedOutput = vectorData.zip(indicies)

    namedRdds.update(output0Name, indicies)
    
    val result = Map(
      "input0" -> input0Name,
      "output0" -> output0Name,
      "centers" -> model.clusterCenters
    )
    return result
  }
}
