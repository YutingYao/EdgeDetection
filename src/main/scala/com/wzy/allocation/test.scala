package com.wzy.allocation

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * 测试设置rdd首选位置
 */
object test {
  def main(args: Array[String]): Unit = {
    val sparkconf: SparkConf = new SparkConf().setAppName("Spark Images EdgeDetection").setMaster("local[2]")
      .set("hdfsBasePath", "hdfs://namenode:8020")
      .set("spark-master", "10.101.241.5")
    //  val sparkconf: SparkConf = new SparkConf().setAppName("Spark Images EdgeDetection").set("hdfsBasePath", "hdfs://namenode:8020")

    val sc: SparkContext = new SparkContext(sparkconf)

    val someData = mutable.ArrayBuffer[(String, Seq[String])]()

    someData += ("1" -> Seq("spark-worker-1"))
    someData += ("2" -> Seq("spark-worker-2"))
    someData += ("3" -> Seq("spark-worker-3"))

    someData.foreach(println)

    val someRdd = sc.makeRDD(someData)

    someRdd.map(i => i + ":" + java.net.InetAddress.getLocalHost().getHostName()).collect().foreach(println)
  }
}
