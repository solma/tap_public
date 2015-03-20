package tap.engine.universalfuncs

import breeze.generic.UFunc
import breeze.linalg.DenseVector
import tap.engine.math

/**
 * Box-Cox universal function.
 */
object BoxCox extends UFunc {

  //TODO(solma): need generic implementation

  implicit object implDV_Double_Double extends Impl2[DenseVector[Double], Double, DenseVector[Double]] {
    def apply(v: DenseVector[Double], lambda: Double) = v.map(math.boxCox(_, lambda))
  }

  implicit object implDV_Double_Int extends Impl2[DenseVector[Double], Int, DenseVector[Double]] {
    def apply(v: DenseVector[Double], lambda: Int) = v.map(math.boxCox(_, lambda))
  }

  implicit object implDV_Long_Double extends Impl2[DenseVector[Long], Double, DenseVector[Double]] {
    def apply(v: DenseVector[Long], lambda: Double) = v.map(math.boxCox(_, lambda))
  }

  implicit object implDV_Long_Int extends Impl2[DenseVector[Long], Int, DenseVector[Double]] {
    def apply(v: DenseVector[Long], lambda: Int) = v.map(math.boxCox(_, lambda))
  }
}
