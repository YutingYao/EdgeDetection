package com.wzy.extend

import geotrellis.raster.Tile
import geotrellis.spark.SpatialKey
import geotrellis.vector.ProjectedExtent
import org.apache.spark.rdd.RDD

import scala.collection.Map
import scala.reflect.ClassTag
import com.wzy._

/**
 * 为myRDD添加隐式函数
 * 添加allocation，使用自适应数据分区策略，对Partition数据进行合理划分
 * Custom functions on myRDD
 */
class RddExtendFunctions[T: ClassTag](rdd: RDD[T]) {

  // 自适应数据分区策略，对Partition数据进行合理划分
  def acllocation(locationPrefs: Map[Int, Seq[String]]) = new MyRDD[T](rdd, locationPrefs)

  // 获取每个分区的大小
  def fetchBuckets: Seq[Bucket] = {
    val partitionSize: Seq[Bucket] = rdd.mapPartitionsWithIndex((index, iterator) => {
      var sum = 0
      for (_ <- iterator) sum += 1
      Iterator(Bucket(index, sum))
    }).collect().toList
    partitionSize
  }

}

object RddImplicit {

  // 隐式视图 把一种类型自动转为另一个类型
  implicit def addCustomFunctions(parent: RDD[(SpatialKey, Tile)]): RddExtendFunctions[(SpatialKey, Tile)] = new RddExtendFunctions(parent)

  implicit def addCustomFunction(parent: RDD[(ProjectedExtent, Tile)]): RddExtendFunctions[(ProjectedExtent, Tile)] = new RddExtendFunctions(parent)

}

