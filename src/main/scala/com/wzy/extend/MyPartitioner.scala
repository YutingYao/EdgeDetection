package com.wzy.extend

import org.apache.spark.Partitioner

class MyPartitioner(val num: Int) extends Partitioner {
  override def numPartitions: Int = num

  override def getPartition(key: Any): Int = {
    // 对tif文件进行分区，以一个tif文件作为一个分区
    // 相对应的就会获取不同大小的partition


    val len: Int = key.toString.length
    //根据单词长度对分区个数取模
    len % num
  }
}
