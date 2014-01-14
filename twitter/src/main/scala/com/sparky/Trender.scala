package com.sparky

import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import com.typesafe.config.ConfigFactory
import twitter4j.Status


/**
 * Created by mike on 1/6/14.
 */
object Trender {
  def main(args: Array[String]) {
    makeAuth()

    val myJar = this.getClass().getProtectionDomain().getCodeSource().getLocation().getPath()
    val spark_home = "../spark-0.8.0-incubating"

    val ssc = new StreamingContext("local", "HashtagTrending", Seconds(5), spark_home, Seq(myJar))
    val tweets = ssc.twitterStream()

    // build the stream
    val statuses = tweets.map(tweet => {
      Map[String, Any]("status" -> tweet.getText())
    })

    val proc: Processor = new TrendingProcessor()
    val saver: Processor = new TrendingStorage("top_count")

    val top_stream = proc.process(statuses)
    saver.process(top_stream)

    ssc.checkpoint("./checkpoint")
    ssc.start()
  }

  def makeAuth() = {
    val conf = ConfigFactory.load()

    System.setProperty("twitter4j.oauth.consumerKey", conf.getString("consumer_key"))
    System.setProperty("twitter4j.oauth.consumerSecret", conf.getString("consumer_secret"))
    System.setProperty("twitter4j.oauth.accessToken", conf.getString("access_token"))
    System.setProperty("twitter4j.oauth.accessTokenSecret", conf.getString("access_token_secret"))
  }
}
