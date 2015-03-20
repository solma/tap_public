package tap.engine.universalfuncs

import breeze.linalg.DenseVector
import breeze.numerics.log
import org.scalatest.FunSuite

class BoxCoxTest extends FunSuite {

  test("Box-Cox universal function") {
    val dv = DenseVector(1.0, 2.0, 3.0)
    assert(BoxCox(dv, 1) == DenseVector(0.0, 1.0, 2.0))
    assert(BoxCox(dv, 0) == log(dv))
  }
}
