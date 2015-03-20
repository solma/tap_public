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
      val avg = mean(v)
      val std = stddev(v)
      v.map(math.normalize(_, avg, std))
    }
  }
}
