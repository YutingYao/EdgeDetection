package com.wzy

import java.awt.image.BufferedImage
import java.text.SimpleDateFormat
import java.util.Date

import com.wzy.monitor.WorkerMonitor
import com.wzy.sharpen.EdgeDetection.detection
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
/**
 * spark 主函数
 */
object EdgeDetection {

  def main(args: Array[String]): Unit = {

    // 建立和Spark框架的连接
    val sparkconf: SparkConf =
      new SparkConf()
        .setAppName("Spark Images EdgeDetection")
        .setMaster("local[2]")
        .set("hdfsBasePath", "hdfs://namenode:8020")
        .set("spark-master", "10.101.241.5")

    val sc: SparkContext = new SparkContext(sparkconf)

    val spark: SparkSession = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    val hdfsBasePath: String = sparkconf.get("hdfsBasePath")

    val inputPath: String = hdfsBasePath + "/input/resample1.tif"

    val outputPath: String = hdfsBasePath + "/output/" + new SimpleDateFormat("yyyyMMddHHmmss").format(new Date())

    //TODO 读取图像数据，存储为DataFrame格式
    //解码后的图像通道顺序和像素数值类型是固定的，顺序固定为BGR(A)，像素数值类型为8U，最多支持4个通道
    val images: DataFrame = spark.read.format("image").option("dropInvalid", value = true).load(inputPath)

    //TODO 对数据分区
    // 1. 获取各个节点的计算资源（CPU核心数量、内存大小数量）
    // 2. 对RDD使用Partition进行分区
    // 3. 调用 getPreferredLocations
    println(sc.applicationId)
    WorkerMonitor.getAllworkers(sc.applicationId, "localhost")

    // 基于rdd格式进行卷积操作
    val FILTER_SCHARR_V: Array[Array[Double]] = Array(Array(3, 0, -3), Array(10, 0, -10), Array(3, 0, -3))

    val inputRdd: RDD[Row] = images.select("image.origin", "image.width", "image.height", "image.nChannels", "image.mode", "image.data").rdd

    println(inputRdd.getNumPartitions)

    val value1 = inputRdd.repartition(10 * inputRdd.getNumPartitions)

    //TODO 对rdd进行分区
    val value = value1.map(row => {
      val origin: String = row.getAs[String]("origin")
      val width: Int = row.getAs[Int]("width")
      val height: Int = row.getAs[Int]("height")
      val mode: Int = row.getAs[Int]("mode")
      val nChannels: Int = row.getAs[Int]("nChannels")
      val data: Array[Byte] = row.getAs[Array[Byte]]("data")
      (origin, height, width, nChannels, mode,
        detection(origin, width, height, BufferedImage.TYPE_USHORT_GRAY, data, FILTER_SCHARR_V))
    })

    value.saveAsObjectFile(outputPath)

    sc.stop()

  }

}
