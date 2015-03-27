package tap.engine

import java.util.HashMap

import com.typesafe.config._
import org.scalatest._
import spark.jobserver._
import FileReader._

class FileReaderSpec extends FlatSpec with Matchers {

  "A FileReader" should "check for missing config" in {
    val conf = ConfigFactory.load()
    val result = FileReader.validate(null, conf)
    result shouldBe a [SparkJobInvalid]
  }
  
  "A FileReader" should "have inputFile and output0 conf parameters" in {
    val confMap: HashMap[String, String]= new HashMap[String, String]()
    confMap.put(InputFileKeyPath, "foo")
    confMap.put(OutputRddNameKeyPath, "bar")
    val conf = ConfigFactory.parseMap(confMap)
    val result = FileReader.validate(null, conf)
    result shouldBe SparkJobValid
  }
}
