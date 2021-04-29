package com.wzy.allocation

import scala.collection.mutable

case class Bucket(var partitionIndex: Int, var size: Int)

case class Effect(var workerId: String, var capability: Int)


// 不考虑shuffle过程
object Allocation {

  private var bks: Seq[Bucket] = _
  private var effs: Seq[Effect] = _
  private var allocation: mutable.Map[Effect, Seq[Bucket]] = _
  private var fit: mutable.Map[Effect, Int] = _


  /**
   * 计算节点负载适应度
   * @param effs
   * @param bks
   * @return
   */
  def Calculate_Fit(effs: Effect, bks: Seq[Bucket]): Double = {
    val rdds: Int = bks.reduce((x, y) => x.size + y.size)
    val allRdds: Int = this.bks.reduce((x, y) => x.size + y.size)
    val capability: Int = effs.capability
    val allCapability: Int = this.effs.reduce((x, y) => x.capability + y.capability)
    (1 / math.pow((rdds / allRdds) - (capability / allCapability), 2)).toInt
  }

  def main(buckets: Seq[Bucket], effects: Seq[Effect]): Unit = {
    bks = buckets.sortBy(_.size)
    effs = effects.sortBy(_.capability)
    var fit_sum: Int = 0
    // 初始化数据结构
    for (i <- effs.indices) {
      allocation.put(effs(i), Seq[Bucket]())
    }
    // 对Bucket进行初始分配
    for (i <- bks.indices) {
      allocation.get(effs(i % effs.length)) match {
        case Some(x) => allocation.update(effs(i % effs.length), x :+ bks(i))
      }
    }
    // 计算各个节点的初始分配适应度
    for (i <- effs.indices) {
      var fit_i: Int = Calculate_Fit(effs(i), allocation.get(effs(i)))
      fit.put(effs(i), fit_i)
      fit_sum += fit_i
    }

    while (true) {
      // 升序排序
      val sortedFit: Seq[(Effect, Int)] = fit.toSeq.sortBy(_._1)
      val under: Effect = sortedFit.head._1
      val over: Effect = sortedFit.last._1

      // 从负载适应度最小的 Reducer 开始，分别选择一个过载与一个负载不足的 Reducer，
      // 从过载的 Reducer 选择最小的 Bucket 移动到负载不足的 Reducer

      //TODO 选取最小的bucket

    }

    def moveMinBucket(): Unit = {

      //TODO 选取最小的Bucket，并进行交换

    }




  }
}
