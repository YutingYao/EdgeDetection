package com.wzy.allocation

/**
 * 分区策略反馈模块
 * 根据partition信息和节点计算能力信息，使用自适应数据分区策略，对Partition数据进行合理划分
 */
class AllocationCenter {
  /**
   * 获取partition list
   * partition index | perfferredlocation
   * 获取node effect
   * node index | node effect
   */
  def allcation(partition: Seq[(String,String)], Effect: Seq[(String,Double)]): Unit = {

  }
}
