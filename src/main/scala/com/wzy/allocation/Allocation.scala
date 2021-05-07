package com.wzy.allocation

import scala.collection.mutable
import scala.util.control._
import com.wzy._
import com.wzy.allocation.AllocationCenter.{pickIndex, random}

import scala.util.Random

/**
 * 分区策略反馈模块
 * 根据partition信息和节点计算能力信息，使用自适应数据分区策略，对Partition数据进行合理划分
 * Max_Min Fairness 算法
 */
object Allocation {

  private var bks: Seq[Bucket] = _
  private var effs: Seq[Effect] = _
  private var TotalRddSize: Int = _
  private var TotalCapability: Int = _
  private val allocation: mutable.Map[Effect, Seq[Bucket]] = mutable.Map()
  private val fit: mutable.Map[Effect, Int] = mutable.Map()

  /**
   * Max_Min Fairness
   *
   * @param buckets
   * @param effects
   */

  def allocate(buckets: Seq[Bucket], effects: Seq[Effect]): mutable.Map[Effect, Seq[Bucket]] = {
    bks = buckets.sortBy(_.size)
    effs = effects.filter(_.workerName != "spark-master").sortBy(_.capability)
    bks.foreach(println)
    effs.foreach(println)
    TotalRddSize = bks.map(_.size).sum
    TotalCapability = effs.map(_.capability).sum
    // 初始化
    for (i <- effs.indices) {
      allocation.put(effs(i), Seq[Bucket]())
    }
    // 对Bucket进行初始分配
    for (i <- bks.indices) {
      allocation.update(effs(i % effs.length), allocation(effs(i % effs.length)) :+ bks(i))
    }
    // 计算各个节点的初始分配适应度
    println("计算各个节点的初始分配适应度")
    var fitSum = calculateFitSum(allocation)
    // 调整各个节点的分配适应度
    println("调整各个节点的分配适应度")
    val loop = new Breaks
    loop.breakable(while (true) {
      val sortedFit: Seq[(Effect, Int)] = fit.toSeq.sortBy(_._2)
      val overEffect: Effect = findOverEffect(sortedFit)
      val underEffect: Effect = findUnderEffect(sortedFit)
      val movedBucket: Bucket = moveMinBucket(overEffect, underEffect)
      // 重新计算fit情况
      val newFitSum = calculateFitSum(allocation)
      allocation.foreach(println)
      fit.foreach(println)
      if (newFitSum <= fitSum) {
        println(s"fitSum: $fitSum; newFitSum: $newFitSum 整体分配适应度没有增加，结束调整")
        moveBackMinBucket(overEffect, underEffect, movedBucket)
        loop.break
      } else {
        println(s"fitSum: $fitSum; newFitSum: $newFitSum 转移Bucket后，整体分配适应度增加，继续调整")
        fitSum = newFitSum
      }
    })
    allocation
  }


  /**
   * 计算节点的任务负载适应度
   *
   * @param effect 节点类
   * @param bks    该节点占据的partitions
   * @return 该节点的任务负载适应度
   */
  def calculateFit(effect: Effect, bks: Seq[Bucket]): Int = {
    val rddSize: Int = bks.map(_.size).sum
    //val rddSize: Int = bks.foldRight(0)((_1, _2) => _2 + _2)
    //println(s"rddSize: $rddSize")
    val capability: Int = effect.capability
    //println(s"capability: $capability")
    // 判断是否过载
    val d = (rddSize.toDouble / TotalRddSize) - (capability.toDouble / TotalCapability)
    val int = (1 / math.pow(d, 2)).toInt
    //println(s"${effect.workerName}; d : $d; fit: $int;")
    int
  }

  /**
   * 计算当前集群环境的任务负载适应度
   *
   * @return 集群整体任务负载适应度
   */
  def calculateFitSum(allocation: mutable.Map[Effect, Seq[Bucket]]): Int = {
    var fitSum: Int = 0
    for ((effect, buckets) <- allocation) {
      var fit_i: Int = calculateFit(effect, buckets)
      fit.update(effect, fit_i)
      fitSum += fit_i
    }
    fitSum
  }

  /**
   * 从过载的 Reducer 选择最小的 Bucket 移动到负载不足的 Reducer
   *
   * @param overEffect  过载节点
   * @param underEffect 负载不足节点
   * @return 最小的partition
   */
  def moveMinBucket(overEffect: Effect, underEffect: Effect): Bucket = {
    //println(s"overEffect: $overEffect; underEffect: $underEffect ")
    val minBucket = allocation(overEffect).minBy(bucket => bucket.size)
    allocation.update(overEffect, allocation(overEffect).filter(_ != minBucket))
    allocation.update(underEffect, allocation(underEffect) :+ minBucket)
    minBucket
  }

  /**
   * rollback moveMinBucket()
   *
   * @param overEffect  过载节点
   * @param underEffect 负载不足节点
   * @param minBucket   最小的partition
   */
  def moveBackMinBucket(overEffect: Effect, underEffect: Effect, minBucket: Bucket): Unit = {
    allocation.update(underEffect, allocation(underEffect).filter(_ != minBucket))
    allocation.update(overEffect, allocation(overEffect) :+ minBucket)
  }

  def findOverEffect(sortedFit: Seq[(Effect, Int)]): Effect = {
    println("findOverEffect")
    for (i <- sortedFit) {
      val buckets = allocation(i._1)
      val rddSize: Int = buckets.map(_.size).sum
      val capability: Int = i._1.capability
      val d = ((rddSize.toDouble / TotalRddSize) - (capability.toDouble / TotalCapability)).signum
      println(s"d : ${d}")
      if (d > 0) return i._1
    }
    println("error")
    sortedFit.head._1
  }

  def findUnderEffect(sortedFit: Seq[(Effect, Int)]): Effect = {
    println("findUnderEffect")
    for (i <- sortedFit) {
      val buckets = allocation(i._1)
      val rddSize: Int = buckets.map(_.size).sum
      val capability: Int = i._1.capability
      val d = ((rddSize.toDouble / TotalRddSize) - (capability.toDouble / TotalCapability)).signum
      println(s"d : ${d}")
      if (d < 0) return i._1
    }
    println("error")
    sortedFit.head._1
  }
}
