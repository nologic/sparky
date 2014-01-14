package com.sparky

import org.apache.spark.streaming.DStream

trait Processor {

  def configure(stream: DStream[Map[String, Any]]) : DStream[Map[String, Any]]

  def process(input: Map[String, Any]): Map[String, Any]

}
