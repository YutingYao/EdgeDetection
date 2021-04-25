package com.wzy.evaluation

import com.wzy.monitor.Worker

/**
 * 计算能力评估模块
 * 利用采集的多项监控数据评估执行节点的计算能力
 * 暂时未使用
 */


object EvaluationCenter {
  private var capability: Seq[Worker] = _
  private var allWorkerCores: Int = _

}
