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

import com.wzy.extend.RddImplicit._


object Cacul {

  def main(args: Array[String]): Unit = {

    val inputfile: String = args(0)
    val multiple: Int = args(1).toInt

    val sparkconf =
      new SparkConf()
        .setAppName("Spark Repartition Application")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryo.registrator", "geotrellis.spark.io.kryo.KryoRegistrator")
        .setIfMissing("spark.kryoserializer.buffer.max", "256m")
        .setIfMissing("spark.kryoserializer.buffer", "64m")
        .setIfMissing("spark.driver.maxResultSize", "4g")
        .set("hdfsBasePath", "hdfs://namenode:8020")
        .set("spark-master", "10.101.241.5")

    val sc = new SparkContext(sparkconf)

    //sc.setLogLevel("ERROR")

    // 获取各个节点的计算能力信息
    println(sc.applicationId)
    val workers: Seq[Worker] = WorkerMonitor.getAllworkers(sc.applicationId, "localhost")

    var coresum = 0
    workers.foreach(x => {
      coresum += x.totalCores
    })


    // HDFS配置
    val hdfsBasePath: String = sparkconf.get("hdfsBasePath")
    val inputPath: String = hdfsBasePath + "/input/" + inputfile
    val outputPath: String = {
      hdfsBasePath + "/output/wzy/" + new SimpleDateFormat("yyyyMMddHHmmss").format(new Date())
    }

    // 对Tiff格式进行解析
    val inputRdd: RDD[(ProjectedExtent, Tile)] = {
      sc.hadoopGeoTiffRDD(inputPath)
    }

    //TODO nputRdd.acllocation()

    import geotrellis.spark.Implicits._
    val (_, rasterMetaData) = {
      TileLayerMetadata.fromRDD(inputRdd, FloatingLayoutScheme(512))
    }

    val tiled: RDD[(SpatialKey, Tile)] = {
      inputRdd.tileToLayout(rasterMetaData.cellType, rasterMetaData.layout, Bilinear)
        .repartition(coresum)
    }

    ////TODO 统计每个分区的大小
    //import com.wzy.extend.RddImplicit._
    //val partitionSize: Seq[(Int, Int)] = tileLayerRDD.fetchPartitionSize
    //partitionSize.foreach(x => println("index: " + x._1 + "  size: " + x._2))

    //TODO 对数据进行分区
    val partitionsIndex = Seq.range(0, multiple * tiled.getNumPartitions)
    val indexToPrefs: Map[Int, Seq[String]] = AllocationCenter.distrbutionByWeight(partitionsIndex, workers)
    import com.wzy.extend.RddImplicit._
    val myrdd: RDD[(SpatialKey, Tile)] = tiled.repartition(multiple * tiled.getNumPartitions).acllocation(indexToPrefs)

    //TODO 执行任务
    myrdd.mapValues { tile =>
      tile.focalMax(Square(3))
    }.saveAsObjectFile(outputPath)

    print("END")

    sc.stop()
  }
}
