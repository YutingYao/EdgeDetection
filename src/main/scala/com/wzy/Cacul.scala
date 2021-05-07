package com.wzy

import java.text.SimpleDateFormat
import java.util.Date

import com.wzy.allocation.Allocation
import com.wzy.extend.RddImplicit._
import com.wzy.monitor.WorkerMonitor
import geotrellis.raster.Tile
import geotrellis.raster.mapalgebra.focal.Square
import geotrellis.raster.resample.Bilinear
import geotrellis.spark._
import geotrellis.spark.io.hadoop.HadoopSparkContextMethodsWrapper
import geotrellis.spark.tiling.FloatingLayoutScheme
import geotrellis.vector.ProjectedExtent
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Cacul {

  def main(args: Array[String]): Unit = {

    val inputfile: String = args(0)
    val multiple: Int = args(1).toInt

    val sparkconf =
      new SparkConf()
        .setMaster("local[*]")
        .setAppName("Spark Repartition Application")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryo.registrator", "geotrellis.spark.io.kryo.KryoRegistrator")
        .setIfMissing("spark.kryoserializer.buffer.max", "256m")
        .setIfMissing("spark.kryoserializer.buffer", "64m")
        .setIfMissing("spark.driver.maxResultSize", "4g")
        .set("hdfsBasePath", "hdfs://namenode:8020")
        .set("spark-master", "10.101.241.5")

    val sc = new SparkContext(sparkconf)

    sc.setLogLevel("ERROR")

    // 获取各个节点的计算能力信息
    println(sc.applicationId)
    //val workers: Seq[Worker] = WorkerMonitor.getAllworkers(sc.applicationId, sparkconf.get("spark-master"))
    val workers: Seq[Worker] = WorkerMonitor.getAllworkers(sc.applicationId, "localhost")
    var clusterTotalCores = 0
    workers.foreach(x => {
      clusterTotalCores += x.totalCores
    })

    //TODO 对节点进行评估
    val effects = evaluation.EvaluationCenter.workersToEffects(workers)

    // HDFS 配置
    //val hdfsBasePath: String = sparkconf.get("hdfsBasePath")
    //val inputPath: String = hdfsBasePath + "/input/" + inputfile
    //val outputPath: String = hdfsBasePath + "/output/wzy/" + new SimpleDateFormat("yyyyMMddHHmmss").format(new Date())

    // LOCAL 配置
    val inputPath = inputfile
    val outputPath: String = "output/" + new SimpleDateFormat("yyyyMMddHHmmss").format(new Date())

    // 对Tiff格式进行解析
    val inputRdd: RDD[(ProjectedExtent, Tile)] = {
      sc.hadoopGeoTiffRDD(inputPath)
    }
    val (_, rasterMetaData) =
      TileLayerMetadata.fromRDD(inputRdd, FloatingLayoutScheme(512))

    val initnumPartitions = inputRdd.getNumPartitions

    val tiled: RDD[(SpatialKey, Tile)] = {
      inputRdd
        .tileToLayout(rasterMetaData.cellType, rasterMetaData.layout, Bilinear)
        .repartition(multiple * initnumPartitions)
    }

    //TODO 通过持久化的rdd来拿数据
    tiled.cache()
    println(tiled.count())
    println(tiled.getNumPartitions)
    println(WorkerMonitor.getRdd(sc.applicationId, "localhost", tiled.id))

    //TODO 统计每个分区的大小
    val buckets: Seq[Bucket] = tiled.fetchBuckets

    //TODO 分区匹配算法
    val indexToPrefs: Map[Int, Seq[String]] = Allocation.allocate(buckets, effects) // Max_Min Fairness 算法

    // val indexToPrefs: Map[Int, Seq[String]] = AllocationCenter.distrbutionByWeight(buckets, workers) // 按权重进行随机分配
    //indexToPrefs.foreach(println)

    val myrdd: RDD[(SpatialKey, Tile)] = tiled.acllocation(indexToPrefs)

    // 任务1
    val count = myrdd.mapValues { tile =>
      tile.focalMax(Square(3))
    }.count()

    print(s"Cacul Application END $count")
    sc.stop()
  }
}
