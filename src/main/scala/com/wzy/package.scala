package com

import scala.collection.mutable

/**
 * 包共有对象
 */
package object wzy {

  case class Bucket(var partitionIndex: Int, var size: Int)

  case class Effect(var workerName: String, var capability: Int)

  case class Worker(var id: String, var hostPort: String, var totalCores: Int, var maxMemory: Double)

  implicit def allocationToPrefs(allocation: mutable.Map[Effect, Seq[Bucket]]): Map[Int, Seq[String]] = {
    var indexToPrefs: Map[Int, Seq[String]] = Map()
    for ((effect, buckets) <- allocation) {
      for(i <- buckets){
        indexToPrefs += (i.partitionIndex -> Seq(effect.workerName))
      }
    }
    indexToPrefs
  }
}
