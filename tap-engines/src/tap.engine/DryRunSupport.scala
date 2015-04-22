package tap.engine

import spark.jobserver.NamedRddSupport
import tap.engine.TapUtil._

/**
 * Trait that allows dry run mode, which enforces safe data type between modules.
 * Output data is generated via mock instead of executing real logic.
 */
trait DryRunSupport {

  /**
   * Transforms on mock data
   * @param mockedInput: mockedInput generated from upstream Rdd
   * @return mocked output
   */
  def convertMockData(mockedInput: Any): Seq[Any]

  /**
   * Generates mock data
   * @param upstreamRddName: upstream (i.e. input) Rdd name (without dry run prefix)
   * @return a Seq to be parallelized by SparkContext
   */
  def genMockData(job: NamedRddSupport, upstreamRddName: Option[String]): Seq[Any] = {
    if (upstreamRddName.isEmpty) {
      convertMockData()
    }
    else {
      val namedRdd = job.namedRdds
      val upstreamRddNameValue = upstreamRddName.get
      val upstreamMockRdd = namedRdd.get[Any](DryRunRddPrefix + upstreamRddNameValue)
      if (upstreamMockRdd.isEmpty) {
        val upstreamRdd = namedRdd.get[Any](upstreamRddNameValue)
        if (upstreamRdd.isEmpty) {
          Seq[Exception](
            new NoSuchElementException("no RDD exits for neither " + upstreamRddName + " nor its mock data"))
        } else {
          convertMockData(upstreamRdd.get.first())
        }
      } else {
        convertMockData(upstreamMockRdd.get.first())
      }
    }
  }
}
