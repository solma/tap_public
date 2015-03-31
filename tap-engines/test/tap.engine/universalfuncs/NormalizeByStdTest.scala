package tap.engine.universalfuncs

import breeze.linalg.DenseVector
import org.scalatest.FunSuite

class NormalizeByStdTest extends FunSuite {

  test("Normalize by standard deviation") {
    val dv = DenseVector(1.0, 2.0, 3.0)
    assert(NormalizeByStd(dv) == DenseVector(-1.0, 0.0, 1.0))
  }
}
