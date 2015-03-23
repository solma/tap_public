package tap.engine.universalfuncs

import breeze.generic.UFunc
import breeze.linalg.DenseVector
import breeze.stats._
import tap.engine.math

/**
 * Normalize a vector by its standard deviation.
 */
object NormalizeByStd extends UFunc{

  implicit object implDV_Double extends Impl[DenseVector[Double], DenseVector[Double]] {
    def apply(v: DenseVector[Double]) = {
      val meanAndVar: MeanAndVariance = meanAndVariance(v)
      v.map(math.normalize(_, meanAndVar.mean, meanAndVar.stdDev))
    }
  }
}
