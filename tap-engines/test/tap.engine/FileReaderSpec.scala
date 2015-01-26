package tap.engine

import spark.jobserver._
import org.apache.spark._
import com.typesafe.config._

import org.scalatest._
import java.util.HashMap

class FileReaderSpec extends FlatSpec with Matchers {

  "A FileReader" should "check for missing config" in {
    val conf = ConfigFactory.load()
    val fileReader = new FileReader()
    val result = fileReader.validate(null, conf)
    result shouldBe a [SparkJobInvalid]
  }

  "A FileReader" should "have inputFile and output0 conf parameters" in {
    val confMap: HashMap[String, String]= new HashMap[String, String]()
    confMap.put("FileReader.inputFile", "foo")
    confMap.put("FileReader.output0", "bar")
    val conf = ConfigFactory.parseMap(confMap)
    System.out.println(conf.getString("FileReader.output0"))
    val fileReader = new FileReader()
    val result = fileReader.validate(null, conf)
    result shouldBe SparkJobValid
  }
}
