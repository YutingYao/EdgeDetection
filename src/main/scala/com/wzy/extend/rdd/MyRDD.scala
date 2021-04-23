package com.wzy.extend.rdd

import scala.collection.Map
import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, TaskContext}

/**
 * Created by wzy on 2021/04/22.
 * 有父依赖的RDD
 * 增加allocation分区功能
 */
class MyRDD[T: ClassTag](parent: RDD[T], locationPrefs: Map[Int, Seq[String]]) extends RDD[T](parent) {

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    parent.iterator(split, context)
  }

  override protected def getPartitions: Array[Partition] = parent.partitions

  /**
   * 获取优先分配的位置
   *
   * @param s
   * @return
   */
  override def getPreferredLocations(s: Partition): Seq[String] = {
    locationPrefs.getOrElse(s.index, Nil)
  }

}
