package tap.engine

/**
 * Trait that allows dry run mode, which enforces safe data type between modules.
 * Output data is generated via mock instead of executing real logic.
 */
trait DryRunSupport {

  /**
   * Generates mock data
   * @param upstreamRddName: upstream (i.e. input) Rdd name (without dry run prefix)
   * @return a Seq to be parallelized by SparkContext
   */
  def genMockData(upstreamRddName: String): Seq[Any]
}
