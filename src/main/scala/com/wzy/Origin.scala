package com.wzy

import java.text.SimpleDateFormat
import java.util.Date

import com.wzy.monitor.{Worker, WorkerMonitor}
import geotrellis.raster.Tile
import geotrellis.raster.mapalgebra.focal.Square
import geotrellis.raster.resample.Bilinear
import geotrellis.spark.{SpatialKey, TileLayerMetadata, withTilerMethods}
import geotrellis.spark.io.hadoop.HadoopSparkContextMethodsWrapper
import geotrellis.spark.tiling.FloatingLayoutScheme
import geotrellis.vector.ProjectedExtent
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object Origin {
  //val maskedPath = "input/Test3.tif"
  //val resultPath = "output/"
  def main(args: Array[String]): Unit = {

    val inputfile: String = args(0)
    val multiple: Int = args(1).toInt

    val sparkconf =
      new SparkConf()
        //.setMaster("local[*]")
        .setAppName("Spark Origin Repartition Application")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryo.registrator", "geotrellis.spark.io.kryo.KryoRegistrator")
        .setIfMissing("spark.kryoserializer.buffer.max", "256m")
        .setIfMissing("spark.kryoserializer.buffer", "64m")
        .setIfMissing("spark.driver.maxResultSize", "4g")
        .set("hdfsBasePath", "hdfs://namenode:8020")
        .set("spark-master", "10.101.241.5")

    val sc = new SparkContext(sparkconf)

    // 获取各个节点的计算能力信息
    println(sc.applicationId)
    val workers: Seq[Worker] = WorkerMonitor.getAllworkers(sc.applicationId, sparkconf.get("spark-master"))

    var coresum = 0
    workers.foreach(x => {
      coresum += x.totalCores
    })


    // HDFS配置
    val hdfsBasePath: String = sparkconf.get("hdfsBasePath")
    val inputPath: String = hdfsBasePath + "/input/" + inputfile
    val outputPath: String = hdfsBasePath + "/output/wzy/" + new SimpleDateFormat("yyyyMMddHHmmss").format(new Date())
    //val outputPath: String = "output/" + new SimpleDateFormat("yyyyMMddHHmmss").format(new Date())

    // 对Tiff格式进行解析
    val inputRdd: RDD[(ProjectedExtent, Tile)] = {
      sc.hadoopGeoTiffRDD(inputPath)
    }

    val (_, rasterMetaData) =
      TileLayerMetadata.fromRDD(inputRdd, FloatingLayoutScheme(512))

    val tiled: RDD[(SpatialKey, Tile)] = {
      inputRdd
        .tileToLayout(rasterMetaData.cellType, rasterMetaData.layout, Bilinear)
        .repartition(multiple * coresum)
    }

    println(tiled.getNumPartitions)

    tiled.cache()

    //TODO 执行任务
    tiled.mapValues { tile =>
      tile.focalMax(Square(3))
    }.saveAsObjectFile(outputPath)

    print(1)

    sc.stop()
  }
}
