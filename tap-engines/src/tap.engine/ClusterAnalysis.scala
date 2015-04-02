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
    val myConf = config.withFallback(ConfigFactory.load())

    val method = myConf.getString("ClusterAnalysis.method")
    val input0Name = myConf.getString("ClusterAnalysis.input0")
    val parsedData = namedRdds.get[org.apache.spark.mllib.linalg.Vector](input0Name).get

    val numClusters = myConf.getInt("ClusterAnalysis.numClusters")
    val numIterations = myConf.getInt("ClusterAnalysis.numIterations")
    val model = KMeans.train(parsedData, numClusters, numIterations)
    val indicies = model.predict(parsedData)

    val clusterSizesFlag = myConf.getBoolean("ClusterAnalysis.withClusterSizes")
    val WSSSEFlag = myConf.getBoolean("ClusterAnalysis.withWSSSE")
    val predictFlag = myConf.hasPath("ClusterAnalysis.output0")

    val os = new ObjectOutputStream(new FileOutputStream("/tmp/example.dat"))
    os.writeObject(model)
    os.close()

    var result = Map(
      "method" -> method,
      "input0" -> input0Name,
      "dataCount" -> parsedData.count(),
      "numClusters" -> numClusters,
      "numIterations" -> numIterations,
      "centers" -> model.clusterCenters
    )

    if (clusterSizesFlag) {
      // If the flag is set, calculate the size for each cluster
      val counters = indicies.map(id => (id, 1)).reduceByKey(_ + _).collect()
      result = result + ("clusterSizes" -> counters)
    }
    if (WSSSEFlag) {
      // Evaluate clustering by computing Within Set Sum of Squared Errors
      val WSSSE = model.computeCost(parsedData)
      result = result + ("WSSSE" -> WSSSE)
    }
    
    result
  }
}
