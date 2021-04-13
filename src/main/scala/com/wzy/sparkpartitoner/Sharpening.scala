package com.wzy.sparkpartitoner

import java.awt.image.{BufferedImage, WritableRaster}
import java.awt.{Color, Font, Graphics2D}
import java.io.{ByteArrayOutputStream, File}
import java.text.SimpleDateFormat
import java.util.Date

import javax.imageio.ImageIO
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * spark 主函数
 */
object Sharpening {

  def main(args: Array[String]): Unit = {

    //TODO 1: 建立和Spark框架的连接
    val sparkconf: SparkConf = new SparkConf().setAppName("Spark Images DataSource").setMaster("local[2]")


    val sc: SparkContext = new SparkContext(sparkconf)

    val spark: SparkSession = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    val hdfsBasePath: String = "E:/IdeaProjects/Sharpening/data"

    val inputPath: String = hdfsBasePath + "/cat/"

    val outputPath: String = hdfsBasePath + "/parquet/" + new SimpleDateFormat("yyyyMMddHHmmss").format(new Date())

    println("input path : " + inputPath)

    println("output path : " + outputPath)

    //TODO 2: 执行业务操作
    //解码后的图像通道顺序和像素数值类型是固定的，顺序固定为BGR(A)，像素数值类型为8U，最多支持4个通道
    val images: DataFrame = spark.read.format("image").option("dropInvalid", value = true).load(inputPath)

    //TODO 3: 对数据分区

    // val imagesRDD: RDD[Row] = images.rdd
    // imagesRDD.partitionBy(new MyPartitioner(4)).mapPartitions(x => x).saveAsTextFile(outputPath)


    //TODO 4: 执行卷积操作
    //236 * 396
    val FILTER_SCHARR_V: Array[Array[Double]] = Array(Array(3, 0, -3), Array(10, 0, -10), Array(3, 0, -3))
    import spark.implicits._
    val value: Dataset[(String, Int, Int, Int, Int, Array[Byte])] = images.select("image.origin", "image.width", "image.height", "image.nChannels", "image.mode", "image.data")
      .map(row => {
        val origin: String = row.getAs[String]("origin")
        val width: Int = row.getAs[Int]("width")
        val height: Int = row.getAs[Int]("height")
        val mode: Int = row.getAs[Int]("mode")
        val nChannels: Int = row.getAs[Int]("nChannels")
        val data: Array[Byte] = row.getAs[Array[Byte]]("data")
        (origin, height, width, nChannels, mode,
//                    markWithText(origin, width, height, BufferedImage.TYPE_3BYTE_BGR, data, "WZY"))
          detection(origin, width, height, BufferedImage.TYPE_3BYTE_BGR, data, FILTER_SCHARR_V))
      })

    //TODO 5: 导出结果
    val value1: DataFrame = value.toDF("origin", "height", "width", "nChannels", "mode", "outData")
    value1.write.format("parquet").save(outputPath)

    //TODO 5: 关闭连接
    sc.stop()

  }

  def markWithText(origin: String, width: Int, height: Int, imageType: Int, data: Array[Byte], text: String): Array[Byte] = {
    val imageName: String = origin.split("/").last
    val image = new BufferedImage(width, height, imageType)
    val raster: WritableRaster = image.getData.asInstanceOf[WritableRaster]
    val pixels: Array[Int] = data.map(_.toInt)
    raster.setPixels(0, 0, width, height, pixels)
    image.setData(raster)
    val buffImg = new BufferedImage(width, height, imageType)
    val g: Graphics2D = buffImg.createGraphics
    g.drawImage(image, 0, 0, null)
    g.setColor(Color.red)
    g.setFont(new Font("宋体", Font.BOLD, 30))
    g.drawString(text, width / 2, height / 2)
    g.dispose()
    val buffer = new ByteArrayOutputStream
    val outputPath = "./data/output/"
    val outfile: File = new File(outputPath + imageName)
    ImageIO.write(buffImg, "JPG", outfile)

    ImageIO.write(buffImg, "JPG", buffer)

    //    print(10086)
    buffer.toByteArray
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

    //TODO 从byte数组 重新构建原图像
    val buffImg = new BufferedImage(width, height, imageType)
    val raster: WritableRaster = buffImg.getData.asInstanceOf[WritableRaster]
    val pixels: Array[Int] = data.map(_.toInt)
    raster.setPixels(0, 0, width, height, pixels)
    buffImg.setData(raster)

    //TODO 获取RGB的三维矩阵
    val imageArray: Array[Array[Array[Double]]] = EdgeDetection.transformImageToArray(buffImg)

    //TODO 对三维矩阵进行卷积
    val finalConv: Array[Array[Double]] = EdgeDetection.applyConvolution(width, height, imageArray, filter)

    //TODO 获取卷积后的图像数据
    val bufferedImage: BufferedImage = EdgeDetection.createBufferImageFromConvolutionMatrix(buffImg, finalConv)

    //TODO 数据导出
    val buffer = new ByteArrayOutputStream
    val outputPath = "./data/output1/"
    val outfile: File = new File(outputPath + imageName)

    ImageIO.write(bufferedImage, "JPG", outfile)

    ImageIO.write(bufferedImage, "JPG", buffer)

    buffer.toByteArray
  }


}

