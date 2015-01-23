package tap.engine

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import scala.util.Try
import spray.json._
import spray.json.DefaultJsonProtocol._
import spark.jobserver._

import java.io.{ObjectOutputStream, ObjectInputStream}
import java.io.{FileOutputStream, FileInputStream}

object ClusterAnalysis extends SparkJob with NamedRddSupport {

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    Try(config.getString("ClusterAnalysis.input0"))
      .map(x => SparkJobValid)
      .getOrElse(SparkJobInvalid("No ClusterAnalysis.input0 config param"))
  }

  override def runJob(sc: SparkContext, config: Config): Any = {
    // Load and parse the data
    val method = config.getString("ClusterAnalysis.method")
    val input0Name = config.getString("ClusterAnalysis.input0")
    val parsedData = namedRdds.get[org.apache.spark.mllib.linalg.Vector](input0Name).get

    // Cluster the data into two classes using KMeans
    val numClusters = config.getInt("ClusterAnalysis.numClusters")
    val numIterations = config.getInt("ClusterAnalysis.numIterations")
    val model = KMeans.train(parsedData, numClusters, numIterations)

    val os = new ObjectOutputStream(new FileOutputStream("/tmp/example.dat"))
    os.writeObject(model)
    os.close()

    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = model.computeCost(parsedData)

    val result = Map(
      "method" -> method,
      "input0" -> input0Name,
      "dataCount" -> parsedData.count(),
      "numClusters" -> numClusters,
      "numIterations" -> numIterations,
      "centers" -> model.clusterCenters,
      "WSSSE" -> WSSSE
    )
    return result
  }
}
