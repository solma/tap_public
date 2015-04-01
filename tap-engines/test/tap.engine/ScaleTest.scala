package tap.engine

import org.apache.spark.mllib.linalg.Vectors
import org.scalatest.{Matchers, GivenWhenThen, FlatSpec}

class ScaleTest extends FlatSpec with TapEngineTestSpec with GivenWhenThen with Matchers {

  "PowerTransform module" should "be tested" in {
    Given("vectors to be normalized")
    val vectors = Array(
      Vectors.dense(1.0, 6.0, 7.0),
      Vectors.dense(2.0, 5.0, 8.0),
      Vectors.dense(3.0, 4.0, 9.0))

    When("normalize")
    val normalizedVectorsWithMean = Scale.normalizeScale(sc.parallelize(vectors), true).collect()
    val normalizedVectorsWithoutMean = Scale.normalizeScale(sc.parallelize(vectors), false).collect()

    Then("normalized verified")
    normalizedVectorsWithMean should equal(Array(
      Vectors.dense(-1.0, 1.0, -1.0),
      Vectors.dense(0.0, 0.0, 0.0),
      Vectors.dense(1.0, -1.0, 1.0)))
    normalizedVectorsWithoutMean should equal(Array(
      Vectors.dense(1.0, 6.0, 7.0),
      Vectors.dense(2.0, 5.0, 8.0),
      Vectors.dense(3.0, 4.0, 9.0)))
  }
}
