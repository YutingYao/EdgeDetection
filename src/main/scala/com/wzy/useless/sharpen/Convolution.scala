package com.wzy.useless.sharpen

/**
 * 卷积运算
 */
class Convolution {
  /**
   * Takes an image (grey-levels) and a kernel and a position,
   * applies the convolution at that position and returns the
   * new pixel value.
   *
   * @param input        The 2D double array representing the image.
   * @param x            The x coordinate for the position of the convolution.
   * @param y            The y coordinate for the position of the convolution.
   * @param k            The 2D array representing the kernel.
   * @param kernelWidth  The width of the kernel.
   * @param kernelHeight The height of the kernel.
   * @return The new pixel value after the convolution.
   */
  def singlePixelConvolution(input: Array[Array[Double]], x: Int, y: Int, k: Array[Array[Double]], kernelWidth: Int, kernelHeight: Int): Double = {
    var output: Double = 0
    for (i <- 0 until kernelWidth) {
      for (j <- 0 until kernelHeight) {
        output = output + (input(x + i)(y + j) * k(i)(j))
      }
    }
    output
  }


  /**
   * Takes a 2D array of grey-levels and a kernel and applies the convolution
   * over the area of the image specified by width and height.
   *
   * @param input        the 2D double array representing the image
   * @param width        the width of the image
   * @param height       the height of the image
   * @param kernel       the 2D array representing the kernel
   * @param kernelWidth  the width of the kernel
   * @param kernelHeight the height of the kernel
   * @return the 2D array representing the new image
   */
  def convolution2D(input: Array[Array[Double]], width: Int, height: Int, kernel: Array[Array[Double]], kernelWidth: Int, kernelHeight: Int): Array[Array[Double]] = {
    val smallWidth: Int = width - kernelWidth + 1
    val smallHeight: Int = height - kernelHeight + 1
    val output: Array[Array[Double]] = Array.ofDim[Double](smallWidth, smallHeight)
    for (i <- 0 until smallWidth) {
      for (j <- 0 until smallHeight) {
        output(i)(j) = 0
      }
    }
    for (i <- 0 until smallWidth) {
      for (j <- 0 until smallHeight) {
        output(i)(j) = singlePixelConvolution(input, i, j, kernel, kernelWidth, kernelHeight)
      }
    }
    output
  }

  /**
   * Takes a 2D array of grey-levels and a kernel, applies the convolution
   * over the area of the image specified by width and height and returns
   * a part of the final image.
   *
   * @param input        the 2D double array representing the image
   * @param width        the width of the image
   * @param height       the height of the image
   * @param kernel       the 2D array representing the kernel
   * @param kernelWidth  the width of the kernel
   * @param kernelHeight the height of the kernel
   * @return the 2D array representing the new image
   */
  def convolution2DPadded(input: Array[Array[Double]], width: Int, height: Int, kernel: Array[Array[Double]], kernelWidth: Int, kernelHeight: Int): Array[Array[Double]] = {
    val smallWidth: Int = width - kernelWidth + 1
    val smallHeight: Int = height - kernelHeight + 1
    val top: Int = kernelHeight / 2
    val left: Int = kernelWidth / 2
    val small: Array[Array[Double]] = convolution2D(input, width, height, kernel, kernelWidth, kernelHeight)
    val large: Array[Array[Double]] = Array.ofDim[Double](width, height)
    for (j <- 0 until height) {
      for (i <- 0 until width) {
//        println(i + "," + j)
        large(i)(j) = 0
      }
    }
    for (j <- 0 until smallHeight) {
      for (i <- 0 until smallWidth) {
        large(i + left)(j + top) = small(i)(j)
      }
    }
    large
  }


  /**
   * Applies the convolution2DPadded  algorithm to the input array as many as
   * iterations.
   *
   * @param input        the 2D double array representing the image
   * @param width        the width of the image
   * @param height       the height of the image
   * @param kernel       the 2D array representing the kernel
   * @param kernelWidth  the width of the kernel
   * @param kernelHeight the height of the kernel
   * @param iterations   the number of iterations to apply the convolution
   * @return the 2D array representing the new image
   */
  def convolutionType2(input: Array[Array[Double]], width: Int, height: Int, kernel: Array[Array[Double]], kernelWidth: Int, kernelHeight: Int, iterations: Int): Array[Array[Double]] = {
    var newInput: Array[Array[Double]] = input.clone
    var output: Array[Array[Double]] = input.clone
    for (_ <- 0 until iterations) {
      output = convolution2DPadded(newInput, width, height, kernel, kernelWidth, kernelHeight)
      newInput = output.clone
    }
    output
  }
}
