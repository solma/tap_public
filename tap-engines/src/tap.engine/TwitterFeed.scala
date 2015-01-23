package tap.engine

import com.typesafe.config.Config
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vectors
import scala.util.Try
import spark.jobserver._

import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.SparkConf
import StreamingContext._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.twitter._
import twitter4j.TwitterFactory
import twitter4j.auth.AccessToken

class TwitterFeed extends SparkJob with NamedRddSupport {

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = SparkJobValid

  override def runJob(sc: SparkContext, config: Config): Any = {

    val consumerKey="XNqmrMcrTEk9JB2FUQC1WPXnH"
    val consumerSecret="05b7whU8V3SIoGqTlRFoQxuzjq3UfLerAxg06J6sCZcemdEzJQ"
    val accessToken="21780328-DcUmKL9UoD8fCsrMlowwFyWTaHTd50g8ejyGjYXYQ"
    val accessTokenSecret="i7M3cOGR8H2Q7P5Dyw42DyfOmxdeBXqplU7lwPodDe6t5"

    // Authorising with your Twitter Application credentials
    val twitter = new TwitterFactory().getInstance()
    twitter.setOAuthConsumer(consumerKey, consumerSecret)
    twitter.setOAuthAccessToken(new AccessToken(accessToken, accessTokenSecret))

    val ssc = new StreamingContext(sc, Seconds(2))
    val filters = Seq("android", "motorola")
    val stream = TwitterUtils.createStream(ssc, Option(twitter.getAuthorization()), filters)

    val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))

    val topCounts60 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(60))
     .map { case (topic, count) => (count, topic) }
     .transform(_.sortByKey(false))

    val topCounts10 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(10))
     .map { case (topic, count) => (count, topic) }
     .transform(_.sortByKey(false))

    // Print popular hashtags
    topCounts60.foreachRDD(rdd => {
      val topList = rdd.take(5)
      println("\nPopular topics in last 60 seconds (%s total):".format(rdd.count()))
      topList.foreach { case (count, tag) => println("%s (%s tweets)".format(tag, count)) }
    })

    topCounts10.foreachRDD(rdd => {
      val topList = rdd.take(5)
      println("\nPopular topics in last 10 seconds (%s total):".format(rdd.count()))
      topList.foreach { case (count, tag) => println("%s (%s tweets)".format(tag, count)) }
    })

    ssc.start()
    ssc.awaitTermination()

    val result = Map(
      "foo" -> "bar"
    )
    return result
  }
}
