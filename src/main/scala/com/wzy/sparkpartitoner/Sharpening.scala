package com.wzy.sparkpartitoner

import java.awt.{Color, Font}
import java.awt.image.{BufferedImage, WritableRaster}
import java.io.{ByteArrayOutputStream, File}
import java.text.SimpleDateFormat
import java.util.Date

import javax.imageio.ImageIO
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}



object Sharpening {

  def main(args: Array[String]): Unit = {

    //TODO 建立和Spark框架的连接
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

    //TODO 执行业务操作
    val images: DataFrame = spark.read.format("image").option("dropInvalid", true).load(inputPath)

    //    val imagesRDD: RDD[Row] = images.rdd

    //    imagesRDD.partitionBy(new MyPartitioner(4)).mapPartitions(x => x).saveAsTextFile(outputPath)

    println(4)
    
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
          markWithText(origin, width, height, BufferedImage.TYPE_3BYTE_BGR, data, "WZY"))
      })

    val value1: DataFrame = value.toDF("origin","height","width","nChannels","mode", "outData")
    value1.write.format("parquet").save(outputPath)

    //TODO 关闭连接

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
    val g = buffImg.createGraphics
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

  def convolution(origin: String, width: Int, height: Int, imageType: Int, data: Array[Byte], k: Array[Array[Int]]): Array[Byte] = {
    val imageName: String = origin.split("/").last
    val buffImg = new BufferedImage(width, height, imageType)
    val raster: WritableRaster = buffImg.getData.asInstanceOf[WritableRaster]
    val pixels: Array[Int] = data.map(_.toInt)

    val myMatrix: Array[Array[Int]] = Array.ofDim[Int](width, height)
    for (i <- 0 to width - 3){
      new Array[Int]()
      for ( j <- 0 to width - 3) {

      }
    }


    raster.setPixels(0, 0, width, height, pixels)
    buffImg.setData(raster)

    val buffer = new ByteArrayOutputStream
    val outputPath = "./data/output/"
    val outfile: File = new File(outputPath + imageName)
    ImageIO.write(buffImg, "PNG", outfile)

    ImageIO.write(buffImg, "PNG", buffer)

    //    print(10086)
    buffer.toByteArray
  }
}
