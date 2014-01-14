package com.sparky

import scala.collection.mutable
import scala.math

/**
 * Created by mike on 1/13/14.
 */

class CountEntity(val key: String, val entity: String) {
  def ==(other: CountEntity):Boolean = {
    key == other.key
  }
}

class CounterEntry(val entity: CountEntity, val delta: Long) {
  private var freq:Long = 1

  def update() {
    freq += 1
  }

  def frequency = freq

  def frequencyDelta = freq + delta
}

// good values are e = 0.0005, s = 0.001
class LossyCounter(e: Double, s: Double) extends Serializable {
  private val se = s - e
  private val bucketWidth = (1.0/e).ceil.toInt

  private var table = new mutable.HashMap[String, CounterEntry]

  private var streamLength:Long = 0
  private var lastStreamLength:Long = 0
  private var currentBucket:Long = 0

  def insert(ent: CountEntity) {
    // insert into the counter
    if(table.contains(ent.key)) {
      val existing:CounterEntry = table.get(ent.key).get

      existing.update()
    } else {
      table.put(ent.key, new CounterEntry(ent, currentBucket - 1))
    }

    table.foreach(p => println(p._1 + " " + p._2.frequency))

    // progress the stream
    streamLength += 1

    if( (streamLength - lastStreamLength) > bucketWidth) {
      lastStreamLength = streamLength
      currentBucket += 1

      // Check if we need to get rid of anything.
      table = prune()
    }
  }

  def getList():List[(CountEntity, Long)] = {
    val qual = se * streamLength.toDouble

    table.filter(pair => {
      println(pair._2.entity + " " + pair._2.frequency + " >= " + qual)
      pair._2.frequency >= qual
    }).map(pair => {
      (pair._2.entity, pair._2.frequency)
    }).toList
  }

  private def prune():mutable.HashMap[String, CounterEntry] = {
    table.filterNot(pair => {
      pair._2.frequencyDelta <= currentBucket
    })
  }
}
