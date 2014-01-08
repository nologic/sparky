package com.sparky

import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import com.typesafe.config.ConfigFactory
import twitter4j.Status


/**
 * Created by mike on 1/6/14.
 */
object Trender extends Processor {
  def main(args: Array[String]) {
    val myJar = this.getClass().getProtectionDomain().getCodeSource().getLocation().getPath()
    val spark_home = "../spark-0.8.0-incubating"

    makeAuth()

    val ssc = new StreamingContext("local", "HashtagTrending", Seconds(1), spark_home, Seq(myJar))

    val tweets = ssc.twitterStream()

    val tweetMaps = tweets.map( tweet => Map("status" -> tweet.getText()) )

    tweetMaps.foreach(rdd => rdd.foreach(statusMap => process(statusMap)))

    /*val statuses = tweets.map(tweet => tweet.getText())
    val hashTags: DStream[String] = statuses.flatMap(status => status.split(" ")).filter(word => word.startsWith("#"))

    val counts = hashTags.map(tag => (tag, 1)).reduceByKeyAndWindow(_ + _, _ - _, Seconds (60 * 5), Seconds(1))

    val sortedCounts = counts.map( (t: (String, Int)) => (t._2, t._1) ).transform(rdd => rdd.sortByKey(false))

    sortedCounts.foreach(rdd =>println("\nTop 10 hashtags:\n" + rdd.take(10).mkString("\n")))*/

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

  def process(input: Map[String, Any]): Map[String, Any] = {
    println("statusMap: " + input)

    input
  }
}
