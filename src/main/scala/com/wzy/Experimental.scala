package com.wzy

import java.text.SimpleDateFormat
import java.util.Date

import com.wzy.allocation.DistributionByMaxminFairness
import com.wzy.allocation.DistributionByWeight
import com.wzy.evaluation.EvaluationCenter
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

import scala.collection.mutable

object Experimental {

  def main(args: Array[String]): Unit = {
    EvaluationCenter.main("10.5.0.2 9002 EvalauationAcotr 10.5.0.2 9000 spark-master".split(" "))

    val distribution: String = args(0)
    val inputfile: String = args(1)
    val multiple: Int = args(2).toInt

    val sparkconf =
      new SparkConf()
        //.setMaster("local[*]")
        .setAppName(s"Experimental Application by $distribution by Expansion $multiple")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryo.registrator", "geotrellis.spark.io.kryo.KryoRegistrator")
        .setIfMissing("spark.kryoserializer.buffer.max", "256m")
        .setIfMissing("spark.kryoserializer.buffer", "64m")
        .setIfMissing("spark.driver.maxResultSize", "4g")
        .set("hdfsBasePath", "hdfs://namenode:8020")
        .set("spark-master", "10.101.241.5")

    val sc = new SparkContext(sparkconf)

    //sc.setLogLevel("ERROR")
    try {
      // 获取各个节点的计算能力信息
      println(sc.applicationId)
      val workers: Seq[Worker] = WorkerMonitor.getAllworkers(sc.applicationId, sparkconf.get("spark-master"))
      var clusterTotalCores = 0
      workers.foreach(x => {
        clusterTotalCores += x.totalCores
      })
      println(EvaluationCenter.workers.size)
      EvaluationCenter.workers.foreach(x => {
        clusterTotalCores += x._2.cpu
        println(s"EvaluationCenter ${x._1} + ${x._2.id} + ${x._2.cpu} + ${x._2.lastCpuUsage} + ${x._2.ram} +  ${x._2.lastMemUsage}")
      })

      //TODO 对节点进行评估
      //val effects = EvaluationCenter.workersToEffects(workers)
      val effects: Seq[Effect] = EvaluationCenter.getEffect

      println("节点评估情况")
      effects.foreach(println)

      // HDFS 配置
      val hdfsBasePath: String = sparkconf.get("hdfsBasePath")
      val inputPath: String = hdfsBasePath + "/input/" + inputfile
      val outputPath: String = hdfsBasePath + "/output/wzy/" + new SimpleDateFormat("yyyyMMddHHmmss").format(new Date())

      // LOCAL 配置
      //val inputPath = inputfile
      //val outputPath: String = "output/" + new SimpleDateFormat("yyyyMMddHHmmss").format(new Date())

      // 对Tiff格式进行解析
      val inputRdd: RDD[(ProjectedExtent, Tile)] = {
        sc.hadoopGeoTiffRDD(inputPath)
      }

      val (_, rasterMetaData) =
        TileLayerMetadata.fromRDD(inputRdd, FloatingLayoutScheme(512))

      val initnumPartitions = inputRdd.getNumPartitions
      println(initnumPartitions)

      val tiled: RDD[(SpatialKey, Tile)] = inputRdd
        .tileToLayout(rasterMetaData.cellType, rasterMetaData.layout, Bilinear)
        .repartition(multiple * initnumPartitions)

      // 获取节点性能权重


      // 统计每个分区的大小
      val buckets: Seq[Bucket] = tiled.fetchBuckets

      // 三种策略进行对比
      distribution match {
        case "origin" => {
          val count = tiled.mapValues { tile =>
            tile.focalMax(Square(3))
          }.count()
          println(s"$distribution  + 计算结果为$count")
        }
        case "weight" => {
          val indexToPrefs = DistributionByWeight.distrbutionByWeight(buckets, workers)
          val myrdd: RDD[(SpatialKey, Tile)] = tiled.acllocation(indexToPrefs)
          val count = myrdd.mapValues { tile =>
            tile.focalMax(Square(3))
          }.count()
          println(s"$distribution  + 计算结果为$count")
        }
        case "maxmin" => {
          val indexToPrefs = DistributionByMaxminFairness.distributionByMaxminFairness(buckets, effects)
          val myrdd: RDD[(SpatialKey, Tile)] = tiled.acllocation(indexToPrefs)
          val count = myrdd.mapValues { tile =>
            tile.focalMax(Square(3))
          }.count()
          println(s"$distribution  + 计算结果为$count")
        }
        case _ =>{
          println(s"Experimental Application by $distribution by Expansion $multiple")
          println(s"$distribution don't match any case error")
        }
      }

      print(s"Experimental Application by $distribution")
    } finally {
      sc.stop()
      EvaluationCenter.stop()
    }
  }
}
