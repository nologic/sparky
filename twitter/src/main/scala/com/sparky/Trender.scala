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

    val ssc = new StreamingContext("local", "HashtagTrending", Seconds(5), spark_home, Seq(myJar))

    val tweets = ssc.twitterStream()

    val statuses = tweets.map(tweet => {
      Map[String, Any]("status" -> tweet.getText())
    })

    val trending = configure(statuses)

    trending.saveAsTextFiles("top_count")
    trending.foreach(rdd =>println("\nTop 10 hashtags:\n" + rdd.take(10).mkString("\n")))

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
    input.get("status").get.toString().split(" ").filter(word => word.startsWith("#")).foreach(tag => println(tag))

    input
  }

  def configure(stream: DStream[Map[String, Any]]): DStream[Map[String, Any]] =
    stream.map(map => {
      // make sure the Strings are not empty

      map.get("status") match {
        case null => ""
        case n => n.toString()
      }
    }).flatMap(status => {
      // Generate words

      status.split(" ")
    }).filter(word => {
      // get those words starting with #

      word.startsWith("#")
    }).map(tag => {
      // Convert works into tuples

      (tag, 1)
    }).reduceByKeyAndWindow(_ + _, _ - _, Seconds (60 * 5), Seconds(5)
     ).map( (t: (String, Int)) => {
      // flip the tuple to have the count as the first term

      (t._2, t._1)
    }).transform(rdd => {
      if(rdd.count() > 0) {
        // pick out only the counts above average

        // compute average
        val average = rdd.map( (p:(Int, String)) => {
          p._1
        }).reduce(_ + _).toDouble / rdd.count().toDouble

        // filter out only those above average
        rdd.filter( (p:(Int, String)) => {
          p._1 >= average
        }).map( (p: (Int, String)) => {
          Map(p._2 -> p._1)
        })
      } else {

        // Just in clase the RDD is empty, we need this
        rdd.map( (p: (Int, String)) => {
          Map(p._2 -> p._1)
        })
      }
    })

}
