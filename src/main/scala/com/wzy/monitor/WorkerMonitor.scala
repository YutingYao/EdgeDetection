package com.wzy.monitor

import scalaj.http.Http
import upickle.default.ReadWriter

/**
 * 节点性能监控模块
 * 采集作业执行相关的节点多项监控信息，为后续的自适应数据分区提供依据
 */

case class Worker(var id: String, var hostPort: String, var totalCores: Int, var maxMemory: Double)

object WorkerMonitor {

  private var workers: Seq[Worker] = _

  /**
   * 获取节点监控信息
   *
   * @param applicationId 任务id
   */
  def getAllworkers(applicationId: String, master: String): Seq[Worker] = {
    val response = Http("http://" + master + ":4040/api/v1/applications/" + applicationId + "/allexecutors").asString

    val json = ujson.read(response.body)

    implicit val workerRW: ReadWriter[Worker] = upickle.default.macroRW[Worker]

    workers = upickle.default.read[Seq[Worker]](json)

    workers.foreach(x => {
      println("=================")
      println("id :" + x.id)
      println("hostPort:" + x.hostPort)
      println("totalCores:" + x.totalCores)
      println("maxMemory: " + x.maxMemory)
    })
    workers
  }
}
