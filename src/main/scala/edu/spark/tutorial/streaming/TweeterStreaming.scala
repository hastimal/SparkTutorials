package edu.spark.tutorial.streaming

/**
 * Created by hastimal on 3/20/2016.
 */
import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TweeterStreaming {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir","F:\\winutils")
    import org.apache.log4j.{Level, Logger}
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val filters = Array("virat", "T20", "IndiaWon", "Kohali", "Dhoni")

    // Twitter Authentication credentials
    System.setProperty("twitter4j.oauth.consumerKey", "t0tAnvsGPStnvRJe6LPOaIjLo")
    System.setProperty("twitter4j.oauth.consumerSecret", "tSeeyiOAfBJqaR9rvAmAt8ePZA3B6YSmymXmcyqeT0FWapPAb0")
    System.setProperty("twitter4j.oauth.accessToken", "1868076104-Zftf4ts0hHGVsnTLYDXneAyTxNXlkoLf86vC3ez")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "GXL9hhOnC9oaSR6xwjEnCuIF01fOaHsYuxo6DH7WcFXty")

    val sparkConf = new SparkConf().setAppName("TweeterStreaming").setMaster("local[*]")
    //Create a Streaming COntext with 5 second window
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    //Using the streaming context, open a twitter stream (By the way you can also use filters)
    //Stream generates a series of random tweets
    val stream = TwitterUtils.createStream(ssc, None, filters)
    stream.print()


    ssc.start()
    ssc.awaitTermination()
  }
}