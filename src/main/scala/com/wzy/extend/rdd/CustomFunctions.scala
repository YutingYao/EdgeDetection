package com.wzy.extend.rdd

import geotrellis.raster.Tile
import geotrellis.spark.SpatialKey
import org.apache.spark.rdd.RDD

import scala.collection.Map

/**
 * 为myRDD添加隐式函数
 * 添加allocation，使用自适应数据分区策略，对Partition数据进行合理划分
 * Custom functions on myRDD
 */
class CustomFunctions(rdd: RDD[(SpatialKey, Tile)]) {

  def acllocation(locationPrefs: Map[Int, Seq[String]]) = new MyRDD(rdd, locationPrefs)

}

object CustomFunctions {

  implicit def addCustomFunctions(parent: RDD[(SpatialKey, Tile)]): CustomFunctions = new CustomFunctions(parent)

}

