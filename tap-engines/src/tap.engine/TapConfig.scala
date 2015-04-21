package tap.engine

import com.typesafe.config.{Config, ConfigException}
import org.apache.spark.{SparkConf, SparkContext}
import spark.jobserver._

import scala.collection.JavaConversions._
import scala.collection.mutable.{Map => MM}
import scala.util.Try

/**
 * Tap Engine config module
 *
 * ===Available configuration parameters===
 * dry_run: boolean
 */
object TapConfig extends SparkJob with NamedRddSupport {

  val ObjectName = this.getClass.getSimpleName.split('$').head
  val DryRunKey = "dry_run"

  val tapConfig = MM[String, String]()

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    Try(!config.isEmpty)
      .map(x => SparkJobValid)
      .getOrElse(SparkJobInvalid(ObjectName + " config is empty"))
  }

  override def runJob(sc: SparkContext, config: Config): Any = {
    config.entrySet().foreach(kv => {
      val key = kv.getKey
      val value = kv.getValue.unwrapped().toString
      key match {
        case tapKey if tapKey.startsWith(ObjectName) => {
          tapConfig.put(key.split('.').tail.mkString("."), value)
          println(key.split('.').tail.mkString(".") + " -> " + value)
        }
        case _ =>
      }
    })
  }

  def isDryRun(): Boolean = tapConfig.get(DryRunKey).getOrElse("False").toBoolean

  def setConfigValue(sc: SparkConf, config: Config, keyName: String): Unit = {
    try {
      val keyValue = config.getString(ObjectName + '.' + keyName)
      println(keyName + " " + keyValue + " " + sc.get(keyName))
      if (keyValue != sc.get(keyName)) {
        sc.set(keyName, keyValue)
        println(sc.get(keyName))
      }
    } catch {
      case e: ConfigException.Missing =>
      case e: ConfigException.WrongType =>
    }
  }
}
