package tap.engine

import scala.math._

/**
 * The package object `tap.engine.math` contains methods for performing additional numeric
 * operations that are not defined in `scala.math` object.
 */
package object math {

  /**
   * Performs Box-Cox transformation.
   *  @param  y value to be transformed.
   *  @param  lambda Box-Cox transformation parameter.
   *  @return transformed value.
   */
  def boxCox(y: Double, lambda: Double): Double = lambda match {
    case 0 => log(y)
    case _ => (pow(y, lambda) - 1) / lambda
  }

  /**
   * Normalizes by standard deviation
   * @param y value to be normalized
   * @param mean population mean
   * @param std population standard deviation
   * @return
   */
  def normalize(y: Double, mean: Double, std: Double): Double = std match {
    case 0 => throw new IllegalArgumentException("standard deviation cannot be zero.")
    case _ => (y - mean) / std
  }
}
