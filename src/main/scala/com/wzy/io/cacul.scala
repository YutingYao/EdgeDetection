package com.wzy.io

import geotrellis.raster.Tile
import geotrellis.raster.mapalgebra.focal.Square
import geotrellis.raster.resample.Bilinear
import geotrellis.spark.io.hadoop.HadoopSparkContextMethodsWrapper
import geotrellis.spark.tiling.FloatingLayoutScheme
import geotrellis.spark.{SpatialKey, TileLayerMetadata, withTilerMethods}
import geotrellis.vector.ProjectedExtent
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object cacul {
  val maskedPath = "input/resample1.tif"
  val resultPath = "output/resample1.png"
  def main(args: Array[String]): Unit = {
    val conf =
      new SparkConf()
        .setMaster("local[*]")
        .setAppName("Spark Tiler")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryo.registrator", "geotrellis.spark.io.kryo.KryoRegistrator")
        .setIfMissing("spark.kryoserializer.buffer.max","256m")
        .setIfMissing("spark.kryoserializer.buffer","64m")
        .setIfMissing("spark.driver.maxResultSize","4g")
    val sc = new SparkContext(conf)

    val inputRdd: RDD[(ProjectedExtent, Tile)] = {
      sc.hadoopGeoTiffRDD(maskedPath)
    }

    val (_, rasterMetaData) =
      TileLayerMetadata.fromRDD(inputRdd, FloatingLayoutScheme(512))

    val tiled: RDD[(SpatialKey, Tile)] = {
      inputRdd
        .tileToLayout(rasterMetaData.cellType, rasterMetaData.layout, Bilinear)
        .repartition(100)
    }


    tiled.mapValues { tile =>
      tile.focalMax(Square(3))
    }.foreach(ST => ST._2.renderPng().write(resultPath))


    print(1)
  }
}
