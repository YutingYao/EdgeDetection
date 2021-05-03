package com.wzy.useless.sharpen

import java.awt.Color
import java.awt.image.{BufferedImage, WritableRaster}
import java.io.ByteArrayOutputStream

import javax.imageio.ImageIO

/**
 * 使用卷积进行边缘检测
 */
object EdgeDetection {

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
    ImageIO.write(bufferedImage, "TIFF", buffer)

    buffer.toByteArray
  }

  /**
   * 需要读取彩色的RGB图像，并从中构建三维矩阵
   *
   * @param bufferedImage BufferedImage
   * @return RGB三维矩阵 ，这部分可能和BGR的排序不同，需要再看看
   */
  def transformImageToArray(bufferedImage: BufferedImage): Array[Array[Array[Double]]] = {
    val width: Int = bufferedImage.getWidth
    val height: Int = bufferedImage.getHeight
    val image: Array[Array[Array[Double]]] = Array.ofDim[Double](3, height, width)
    for (i <- 0 until height) {
      for (j <- 0 until width) {
        val color = new Color(bufferedImage.getRGB(j, i))
        image(0)(i)(j) = color.getRed
        image(1)(i)(j) = color.getGreen
        image(2)(i)(j) = color.getBlue
      }
    }
    image
  }

  /**
   * 使用特定的滤波器对每个矩阵R，G，B进行卷积运算
   *
   * @param width  图像宽度
   * @param height 图像高度
   * @param image  图像的RGB矩阵
   * @param filter 滤波器
   * @return 卷积后的图像矩阵
   */
  def applyConvolution(width: Int, height: Int, image: Array[Array[Array[Double]]], filter: Array[Array[Double]]): Array[Array[Double]] = {
    val convolution = new Convolution()
    val redConv: Array[Array[Double]] = convolution.convolutionType2(image(0), height, width, filter, 3, 3, 1)
    val greenConv: Array[Array[Double]] = convolution.convolutionType2(image(1), height, width, filter, 3, 3, 1)
    val blueConv: Array[Array[Double]] = convolution.convolutionType2(image(2), height, width, filter, 3, 3, 1)
    val finalConv: Array[Array[Double]] = Array.ofDim[Double](redConv.length, redConv(0).length)
    //sum up all convolution outputs
    for (i <- redConv.indices) {
      for (j <- redConv(i).indices) {
        finalConv(i)(j) = redConv(i)(j) + greenConv(i)(j) + blueConv(i)(j)
      }
    }
    finalConv
  }

  /**
   * 将卷积后的图像矩阵转为Byte数组
   *
   * @param bufferedImage 原图像矩阵
   * @param imageRGB      卷积后的图像矩阵
   * @return byte数组形式表示的图像数据
   */
  def createBufferImageFromConvolutionMatrix(bufferedImage: BufferedImage, imageRGB: Array[Array[Double]]): BufferedImage = {
    val writeBackBufferImage = new BufferedImage(bufferedImage.getWidth, bufferedImage.getHeight, BufferedImage.TYPE_INT_RGB)
    val raster: WritableRaster = writeBackBufferImage.getData.asInstanceOf[WritableRaster]
    for (i <- imageRGB.indices) {
      for (j <- imageRGB(i).indices) {
        val color = new Color(fixOutOfRangeRGBValues(imageRGB(i)(j)), fixOutOfRangeRGBValues(imageRGB(i)(j)), fixOutOfRangeRGBValues(imageRGB(i)(j)))
        writeBackBufferImage.setRGB(j, i, color.getRGB)
      }
    }
    writeBackBufferImage
  }

  /**
   * RGB的数值限定为0 ~255
   *
   * @param value 卷积后的像素值
   * @return 0 ~ 255 之间的像素值
   */
  private def fixOutOfRangeRGBValues(value: Double): Int = {
    var v: Double = value
    if (v < 0.0) v = math.abs(v)
    if (v > 255) 255
    else v.toInt
  }
}
