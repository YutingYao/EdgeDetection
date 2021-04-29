package com.wzy.allocation

import scala.collection.mutable

case class Bucket(var partitionIndex: Int, var size: Int)
case class Effect(var workerId: String, var capability: Int)


object Allocation {
  def main(buckets: Seq[Bucket], effects: Seq[Effect]): Unit = {
    val bks = buckets.sortBy(_.size)
    val effs = effects.sortBy(_.capability)
    val allcation = mutable.HashMap[Effect, Seq[Bucket]]()
    val fit = mutable.HashMap[Effect, Int]()
    for (i <- effs.indices){
      allcation.put(effs(i), mutable.Seq[Bucket]())
    }
    //for(i <- bks.indices){
    //  fit_i
    //}
  }
}
