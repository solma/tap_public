package tap.engine.core

import com.typesafe.config.Config
import org.apache.spark.SparkContext
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
    val result = MM[String, String]()
    config.entrySet().foreach(kv => {
      val key = kv.getKey
      val value = kv.getValue.unwrapped().toString
      key match {
        case tapKey if tapKey.startsWith(ObjectName) => {
          val property = key.split('.').tail.mkString(".")
          tapConfig.put(property, value)
          result.put(property, value)
        }
        case _ =>
      }
    })
    result
  }

  def isDryRun(): Boolean = tapConfig.get(DryRunKey).getOrElse("False").toBoolean
}
