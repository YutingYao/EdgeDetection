package com.wzy.edgedetection

import java.awt.image.{BufferedImage, WritableRaster}
import java.io.{ByteArrayOutputStream, File}
import java.text.SimpleDateFormat
import java.util.Date

import javax.imageio.ImageIO
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * spark 主函数
 */
object Application {

  def main(args: Array[String]): Unit = {

    // 建立和Spark框架的连接
    val sparkconf: SparkConf = new SparkConf().setAppName("Spark Images EdgeDetection").setMaster("local[2]").set("hdfsBasePath", "hdfs://namenode:8020")
    //  val sparkconf: SparkConf = new SparkConf().setAppName("Spark Images EdgeDetection").set("hdfsBasePath", "hdfs://namenode:8020")

    val sc: SparkContext = new SparkContext(sparkconf)

    val spark: SparkSession = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    val hdfsBasePath: String = "."
    //  val hdfsBasePath: String = sparkconf.get("hdfsBasePath")

    val inputPath: String = hdfsBasePath + "/input/cat/"

    val outputPath: String = hdfsBasePath + "/output/parquet/" + new SimpleDateFormat("yyyyMMddHHmmss").format(new Date())

    println("input path : " + inputPath)

    println("output path : " + outputPath)

    //TODO 读取图像数据，存储为DataFrame格式
    //解码后的图像通道顺序和像素数值类型是固定的，顺序固定为BGR(A)，像素数值类型为8U，最多支持4个通道
    val images: DataFrame = spark.read.format("image").option("dropInvalid", value = true).load(inputPath)

    //TODO 对数据分区
    // 1. 获取各个节点的计算资源（CPU核心数量、内存大小数量）
    // 2. 对RDD使用Partition进行分区
    // 3. 调用 getPreferredLocations
    println(sc.applicationId)

    val taskMonitor = new WorkerMonitor()
    taskMonitor.getAllworkers(sc.applicationId, "10.101.241.5")


    // val imagesRDD: RDD[Row] = images.rdd
    // imagesRDD.partitionBy(new MyPartitioner(4)).mapPartitions(x => x).saveAsTextFile(outputPath)

    // 执行卷积操作
    //TODO 以下步骤应该改为基于rdd格式运行
    val FILTER_SCHARR_V: Array[Array[Double]] = Array(Array(3, 0, -3), Array(10, 0, -10), Array(3, 0, -3))


    import spark.implicits._
    //    val value: Dataset[(String, Int, Int, Int, Int, Array[Byte])] = images.select("image.origin", "image.width", "image.height", "image.nChannels", "image.mode", "image.data")
    //      .map(row => {
    //        val origin: String = row.getAs[String]("origin")
    //        val width: Int = row.getAs[Int]("width")
    //        val height: Int = row.getAs[Int]("height")
    //        val mode: Int = row.getAs[Int]("mode")
    //        val nChannels: Int = row.getAs[Int]("nChannels")
    //        val data: Array[Byte] = row.getAs[Array[Byte]]("data")
    //        (origin, height, width, nChannels, mode,
    //          detection(origin, width, height, BufferedImage.TYPE_3BYTE_BGR, data, FILTER_SCHARR_V))
    //      })

    val imagesrdd: RDD[Row] = images.select("image.origin", "image.width", "image.height", "image.nChannels", "image.mode", "image.data").rdd

    //TODO 对rdd进行分区
    val value = imagesrdd.map(row => {
      val origin: String = row.getAs[String]("origin")
      val width: Int = row.getAs[Int]("width")
      val height: Int = row.getAs[Int]("height")
      val mode: Int = row.getAs[Int]("mode")
      val nChannels: Int = row.getAs[Int]("nChannels")
      val data: Array[Byte] = row.getAs[Array[Byte]]("data")
      (origin, height, width, nChannels, mode,
        detection(origin, width, height, BufferedImage.TYPE_3BYTE_BGR, data, FILTER_SCHARR_V))
    })
    println(value)
    // 导出结果
    val value1: DataFrame = value.toDF("origin", "height", "width", "nChannels", "mode", "outData")
    value1.write.format("parquet").save(outputPath)

    // 关闭连接
    sc.stop()

  }

  /**
   * 对图像进行卷积处理
   *
   * @param originImageURL 原图像的url
   * @param width          图像宽度
   * @param height         图像高度
   * @param imageType      图像格式
   * @param data           byte数组形式的图像数据
   * @param filter         滤波器
   * @return byte数组形式的图像数据
   */
  def detection(originImageURL: String, width: Int, height: Int, imageType: Int, data: Array[Byte], filter: Array[Array[Double]]): Array[Byte] = {
    val imageName: String = originImageURL.split("/").last

    // 从byte数组 重新构建原图像
    val buffImg = new BufferedImage(width, height, imageType)
    val raster: WritableRaster = buffImg.getData.asInstanceOf[WritableRaster]
    val pixels: Array[Int] = data.map(_.toInt)
    raster.setPixels(0, 0, width, height, pixels)
    buffImg.setData(raster)

    // 获取RGB的三维矩阵
    val imageArray: Array[Array[Array[Double]]] = EdgeDetection.transformImageToArray(buffImg)

    // 对三维矩阵进行卷积
    val finalConv: Array[Array[Double]] = EdgeDetection.applyConvolution(width, height, imageArray, filter)

    // 获取卷积后的图像数据
    val bufferedImage: BufferedImage = EdgeDetection.createBufferImageFromConvolutionMatrix(buffImg, finalConv)

    // 数据导出
    val buffer = new ByteArrayOutputStream
    ImageIO.write(bufferedImage, "JPG", buffer)

    // val outputPath = "./data/output2/"
    // val outfile: File = new File(outputPath + imageName)
    // ImageIO.write(bufferedImage, "JPG", outfile)

    buffer.toByteArray
  }

}

