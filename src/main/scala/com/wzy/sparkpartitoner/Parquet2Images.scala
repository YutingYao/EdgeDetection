package com.wzy.sparkpartitoner

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object Parquet2Images {
  def main(args: Array[String]): Unit = {
    //TODO 建立和Spark框架的连接
    val sparkconf: SparkConf = new SparkConf().setAppName("Spark Images DataSource").setMaster("local[2]")


    val sc: SparkContext = new SparkContext(sparkconf)

    val spark: SparkSession = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    val InputPath: String = "/data/input"
    val userDF = spark.read.parquet("file:///usr/local/Cellar/spark-2.3.0/examples/src/main/resources/users.parquet")


    sc.stop()

  }
}
