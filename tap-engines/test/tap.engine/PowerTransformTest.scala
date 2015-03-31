package tap.engine

import org.apache.spark.mllib.linalg.{Vectors}
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}

class PowerTransformTest extends FlatSpec with TapEngineTestSpec with GivenWhenThen with Matchers {

  "PowerTransform module" should "be tested" in {
    Given("vectors to be transformed")
    val vectors = Array(Vectors.dense(1.0, 2.0, 3.0))

    Given("lambda vector")
    val lambdaVector = Vector(0.0, -1.0, 1.0)

    When("transform")
    val transformedVectors = PowerTransform.boxCox(sc.parallelize(vectors), lambdaVector).collect()

    Then("transformed verified")
    transformedVectors should equal(Array(Vectors.dense(0.0, 0.5, 2.0)))
  }
}
