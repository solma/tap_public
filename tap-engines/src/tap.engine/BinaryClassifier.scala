package tap.engine

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLUtils
import scala.util.Try
import spark.jobserver._
// import spray.json._
// import spray.json.DefaultJsonProtocol._

class BinaryClassifier extends SparkJob with NamedRddSupport {

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    Try(config.getString("BinaryClassifier.input0"))
      .map(x => SparkJobValid)
      .getOrElse(SparkJobInvalid("No BinaryClassifier.input0 config param"))
  }

  override def runJob(sc: SparkContext, config: Config): Any = {
    // Load and parse the data
    val method = config.getString("BinaryClassifier.method")
    val numIterations = config.getInt("BinaryClassifier.numIterations")
    val testSplit = config.getDouble("BinaryClassifier.testSplit")
    val input0Name = config.getString("BinaryClassifier.input0")
    // val data = MLUtils.loadLibSVMFile(sc, "/tmp/sample_libsvm_data.txt")
    // val data = MLUtils.loadLibSVMFile(sc, "/tmp/breast-cancer_svm.txt")
    val data = namedRdds.get[org.apache.spark.mllib.regression.LabeledPoint](input0Name).get

    val splits = data.randomSplit(Array(1.0 - testSplit, testSplit), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)

    // Run training algorithm to build the model
    val model = SVMWithSGD.train(training, numIterations)

    // Clear the default threshold.
    model.clearThreshold()

    // Compute raw scores on the test set. 
    val scoreAndLabels = test.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }

    // Get evaluation metrics.
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val auROC = metrics.areaUnderROC()

    val result = Map(
      "input0" -> input0Name,
      "count" -> data.count(),
      "numIterations" -> numIterations,
      "testSplit" -> testSplit,
      "auROC" -> auROC
    )
    return result
  }
}
