package com.wzy.evaluation

import com.wzy.monitor.Worker

/**
 * 计算能力评估模块
 * 利用采集的多项监控数据评估执行节点的计算能力
 */


object EvaluationCenter {
  private var workers: Seq[Worker] = _

  /**
   * 对节点计算性能进行评估
   *
   * @param worker
   * @return
   */
  def evaluateComputingPower(worker: Worker): Int = {
    worker.totalCores
  }

}
