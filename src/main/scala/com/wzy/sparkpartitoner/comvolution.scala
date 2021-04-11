package com.wzy.sparkpartitoner

import java.awt.Color
import java.awt.image.{BufferedImage, WritableRaster}

object convolution {
  def main(args: Array[String]): Unit = {

  }

  def convolution(width: Int, height: Int, imageType: Int, data: Array[Byte], k: Array[Array[Int]]): Array[Int] = {
    val image = new BufferedImage(width, height, imageType)
    val raster: WritableRaster = image.getData.asInstanceOf[WritableRaster]
    val pixels: Array[Int] = data.map(_.toInt)
    raster.setPixels(0, 0, width, height, pixels)
    image.setData(raster)
    pixels
  }

  // 获取图像的每个像素的内容
  def transformImageToArray(width: Int, height: Int, pixels: Array[Int]): Array[Array[Array[Int]]] = {
    val image: Array[Array[Array[Int]]] = Array.ofDim[Int](3, height, width)
    var index = 0
    for (i <- 0 to height - 1) {
      for (j <- 0 to width - 1) {
        val color: Color = new Color(pixels(index))
        image(0)(i)(j) = color.getRed();
        image(1)(i)(j) = color.getGreen();
        image(2)(i)(j) = color.getBlue();
        index += 1
      }
    }
    image;
  }

  def applyConvolution(width: Int, height: Int, image: Array[Array[Array[Int]]], filter:Array[Array[Double]]): Unit = {

  }
}
