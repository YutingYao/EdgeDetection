package com.wzy.sharpen

import java.io.File

import javax.imageio.ImageIO

object readtiff {
  def main(args: Array[String]): Unit = {
    val file: File = new File("input/B02-RM.tif")
    val bufferedImage = ImageIO.read(file)
    println(bufferedImage.getType)
  }
}
