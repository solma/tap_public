package tap.engine.math

import org.scalatest.{Matchers, FlatSpec}

class package$Test extends FlatSpec with Matchers {

  "Box-Cox transform" should "calculate correct" in {
    boxCox(math.E, 0) should be (1)
    boxCox(1, 0) should be (math.log(1))
    boxCox(100, 1) should be (99)
    boxCox(9, 2) should be (40)
  }

  "Normalization by standard deviation" should "calculate correct" in {
    normalize(2, 1, 1) should be(1)
    normalize(2, 0, 2) should be(1)
    intercept[IllegalArgumentException] {
      normalize(2, 1, 0)
    }
  }
}
