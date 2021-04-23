package com.wzy

import java.text.SimpleDateFormat
import java.util.Date

import com.wzy.monitor.WorkerMonitor
import geotrellis.raster.Tile
import geotrellis.raster.mapalgebra.focal.Square
import geotrellis.raster.resample.Bilinear
import geotrellis.spark.io.hadoop.HadoopSparkContextMethodsWrapper
import geotrellis.spark.tiling.FloatingLayoutScheme
import geotrellis.spark.{SpatialKey, TileLayerMetadata, withTilerMethods}
import geotrellis.vector.ProjectedExtent
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Cacul {
  //val maskedPath = "input/resample1.tif"
  //val resultPath = "output/resample1.png"

  def main(args: Array[String]): Unit = {
    val sparkconf =
      new SparkConf()
        //.setMaster("local[*]")
        .setAppName("Spark Repartition Application")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryo.registrator", "geotrellis.spark.io.kryo.KryoRegistrator")
        .setIfMissing("spark.kryoserializer.buffer.max", "256m")
        .setIfMissing("spark.kryoserializer.buffer", "64m")
        .setIfMissing("spark.driver.maxResultSize", "4g")
        .set("hdfsBasePath", "hdfs://namenode:8020")
        .set("spark-master", "10.101.241.5")

    val sc = new SparkContext(sparkconf)

    val hdfsBasePath: String = sparkconf.get("hdfsBasePath")

    val inputPath: String = hdfsBasePath + "/input/B02-RM.tif"

    val outputPath: String = hdfsBasePath + "/output/wzy/" + new SimpleDateFormat("yyyyMMddHHmmss").format(new Date())

    val inputRdd: RDD[(ProjectedExtent, Tile)] = {
      sc.hadoopGeoTiffRDD(inputPath)
    }

    val (_, rasterMetaData) =
      TileLayerMetadata.fromRDD(inputRdd, FloatingLayoutScheme(512))

    val tiled: RDD[(SpatialKey, Tile)] = {
      inputRdd
        .tileToLayout(rasterMetaData.cellType, rasterMetaData.layout, Bilinear)
        .repartition(100)
    }

    println(tiled.getNumPartitions)
    //获取各个节点的计算能力信息
    println(sc.applicationId)
    WorkerMonitor.getAllworkers(sc.applicationId, sparkconf.get("spark-master"))

    //获取partitions的数量和每个分区的大小
    println(tiled.getNumPartitions)

    //TODO  获取seq列表  rdd分区和节点对应的列表
    var indexToPrefs: Map[Int, Seq[String]] = Map()

    for (i <- 1 to tiled.getNumPartitions) {
      indexToPrefs += (i -> Seq("spark-worker-1"))
    }

    //TODO 获取
    import com.wzy.extend.rdd.CustomFunctions._
    val myrdd = tiled.acllocation(indexToPrefs)

    myrdd.cache()
    myrdd.map(i => i + ":" + java.net.InetAddress.getLocalHost().getHostName()).collect().foreach(println)
    //myrdd.mapValues { tile =>
    //  tile.focalMax(Square(3))
    //}.foreach(ST => ST._2.renderPng().write(outputPath))

    print(1)
  }
}
