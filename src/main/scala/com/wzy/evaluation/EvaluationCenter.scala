package com.wzy.evaluation

import com.wzy._

/**
 * 计算能力评估模块
 * 利用采集的多项监控数据评估执行节点的计算能力
 */


object EvaluationCenter {
  def toEffect(worker: Worker): Effect = Effect(worker.hostPort.split(":")(0), worker.totalCores)

  def workersToEffects(workers: Seq[Worker]): Seq[Effect] = {
    workers.map(x => toEffect(x))
  }

}
