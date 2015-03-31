package tap.engine

import java.util.HashMap

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark._
import org.apache.spark.mllib.random.RandomRDDs._
import spark.jobserver._

import scala.util.Try

object RandomData extends SparkJob with NamedRddSupport {

  val ObjectName = this.getClass.getSimpleName.split('$').head
  val DistributionKey = ObjectName + ".mode"
  val NumberOfRowsKey = ObjectName + ".numRows"
  val NumberOfColumnsKey = ObjectName + ".numCols"
  val OutputRddKeyName = ObjectName + ".output0"

  def main(args: Array[String]) {
    val sc = new SparkContext("local[4]", "WordCountExample")

    val confMap: HashMap[String, String]= new HashMap[String, String]()
    confMap.put(DistributionKey, "uniformVector")
    confMap.put(NumberOfRowsKey, "10")
    confMap.put(NumberOfColumnsKey, "10")
    confMap.put(OutputRddKeyName, "test")

    val config = ConfigFactory.parseMap(confMap)
    val results = runJob(sc, config)
    println("Result is " + results)
  }

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    Try(config.getString("RandomData.mode"))
      .map(x => SparkJobValid)
      .getOrElse(SparkJobInvalid("No RandomData.mode config param"))
    Try(config.getString("RandomData.output0"))
      .map(x => SparkJobValid)
      .getOrElse(SparkJobInvalid("No RandomData.out0 config param"))
  }

  override def runJob(sc: SparkContext, config: Config): Any = {
    val mode = config.getString(DistributionKey)
    val output0Name = config.getString(OutputRddKeyName)
    val numRows = config.getLong(NumberOfRowsKey)
    val numCols = config.getInt(NumberOfColumnsKey)

    val randomRdd = mode match {
      case "uniformVector" => uniformVectorRDD(sc, numRows, numCols)
      case "normalVector" => normalVectorRDD(sc, numRows, numCols)
    }

    namedRdds.update(output0Name, randomRdd)

    val result = Map(
      "output0" -> output0Name
    )
    result
  }
}
