package tap.engine

import org.apache.spark.SparkContext

/**
 * Constants and methods that are shared by all modules.
 */
object TapUtil {
  val DryRunKey = "spark.dry.run"
  val DryRunRddPrefix = "__"

  val InputRddKey = "input0"
  val OutputRddKey = "output0"

  def isDryRun(sc: SparkContext): Boolean = sc.getConf.get(TapUtil.DryRunKey).toBoolean
}
