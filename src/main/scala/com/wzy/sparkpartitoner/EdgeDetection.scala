package com.wzy.sparkpartitoner

import java.awt.Color
import java.awt.image.WritableRaster
import java.io.IOException

import com.wzy.utils.Convolution

/**
 * 使用卷积进行边缘检测
 */
object EdgeDetection {

  import java.awt.image.BufferedImage

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
   * @param bufferedImage 原图像矩阵
   * @param imageRGB 卷积后的图像矩阵
   * @return byte数组形式表示的图像数据
   */
  def createBufferImageFromConvolutionMatrix(bufferedImage: BufferedImage, imageRGB: Array[Array[Double]]): BufferedImage = {
    val writeBackBufferImage = new BufferedImage(bufferedImage.getWidth,bufferedImage.getHeight, BufferedImage.TYPE_INT_RGB)
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
