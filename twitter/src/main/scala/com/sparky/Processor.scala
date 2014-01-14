package com.sparky

import org.apache.spark.streaming.DStream

trait Processor {

  def process(stream: DStream[Map[String, Any]]) : DStream[Map[String, Any]]

}
