package com.wzy

import java.text.SimpleDateFormat
import java.util.Date

import geotrellis.raster.Tile
import geotrellis.raster.mapalgebra.focal.Square
import geotrellis.raster.resample.Bilinear
import geotrellis.spark.io.hadoop.HadoopSparkContextMethodsWrapper
import geotrellis.spark.tiling.FloatingLayoutScheme
import geotrellis.spark._
import geotrellis.vector.ProjectedExtent
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import com.wzy.allocation.AllocationCenter
import com.wzy.monitor.{Worker, WorkerMonitor}


object Cacul {
  //  val maskedPath = "input/resample1.tif"
  //  val resultPath = "output/"

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

    // 获取各个节点的计算能力信息
    println(sc.applicationId)
    val workers: Seq[Worker] = WorkerMonitor.getAllworkers(sc.applicationId, sparkconf.get("spark-master"))

    var coresum = 0
    workers.foreach(x => {
      coresum += x.totalCores
    })


    // HDFS配置
    val hdfsBasePath: String = sparkconf.get("hdfsBasePath")
    val inputPath: String = hdfsBasePath + "/input/B02-RM.tif"
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
        .repartition(4 * coresum)
    }

    println(tiled.getNumPartitions)

    //TODO 对数据进行分区
    val partitionsIndex = Seq.range(0, tiled.getNumPartitions)
    val indexToPrefs: Map[Int, Seq[String]] = AllocationCenter.distrbutionByWeight(partitionsIndex, workers)

    indexToPrefs.foreach(println)

    import com.wzy.extend.rdd.CustomFunctions._
    val myrdd = tiled.acllocation(indexToPrefs)

    myrdd.cache()
    myrdd.map(i => i + ":" + java.net.InetAddress.getLocalHost().getHostName()).collect().foreach(println)

    //TODO 执行任务
    myrdd.mapValues { tile =>
      tile.focalMax(Square(3))
    }.saveAsObjectFile(outputPath)

    print(1)

    sc.stop()
  }
}
