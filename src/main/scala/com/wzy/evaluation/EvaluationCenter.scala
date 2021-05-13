package com.wzy.evaluation

import akka.actor.{Actor, ActorRef, ActorSelection, ActorSystem, Props}
import com.wzy.akka.common.WorkerInfo
import com.typesafe.config.ConfigFactory

import scala.collection.mutable
import scala.concurrent.duration._

/**
 * 计算能力评估模块
 * 利用采集的多项监控数据评估执行节点的计算能力
 */

class EvaluationActor(workerName: String, serverHost: String, serverPort: Int, masterName: String) extends Actor {
  //定义一个MasterActorRef
  var masterActorProxy: ActorSelection = _

  //定义Worker的编号
  var id: String = "EvaluationActor"

  //在Actor中有一个方法preStart方法，它会在Actor运行前执行
  //在Akka开发中，通常将初始化的工作，放在preStart方法中
  override def preStart(): Unit = {
    this.masterActorProxy = context.actorSelection("akka.tcp://Master@" + serverHost + ":" + serverPort + "/user/" + masterName)
    println("this.masterActorProxy=" + this.masterActorProxy)
  }

  override def receive: Receive = {
    case "start" =>
      println("Evaluation Actor 启动运行，启动计时器，每3s更新一次数据")
      import context.dispatcher
      context.system.scheduler.schedule(0 millis, 5000 millis, self, "GetMasterActorWorkers")
      masterActorProxy ! "RegisterEvaluation"
    case "GetMasterActorWorkers" =>
      // "向Master Monitor发送请求"
      masterActorProxy ! "GetMasterActorWorkers"
    case workers: Map[String, WorkerInfo] =>
      // 更新Evaluation 的worker情况
      println("更新Evaluation 的worker情况")
      workers.foreach(x => {
        println(s"receive info ${x._1} + ${x._2.id} + ${x._2.cpu} + ${x._2.lastCpuUsage} + ${x._2.ram} +  ${x._2.lastMemUsage}")
      })
      EvaluationCenter.evaluateAbilities(workers: Map[String, WorkerInfo])
      EvaluationCenter.workerToWeight.foreach(x => {
        println(s"EvaluationCenter workerToWeight ${x._1} + ${x._2}")
      })

    case "stop" =>
      context.system.terminate()
  }
}

object EvaluationCenter {
  var workerToWeight: Map[String, Int] = Map()

  var workers: Map[String, WorkerInfo] = Map()

  var workerActorRef: ActorRef = _

  import com.wzy._
  def toEffect(k: String, v: Int): Effect = Effect(k,v)

  def stop(): Unit = {
    this.workerActorRef ! "stop"
  }

  def evaluateAbilities(workers: Map[String, WorkerInfo]): Map[String, Int] = {
    // Todo 分配初始化权重 即CPU核数
    // 遍历节点类型， 获取workerInfoAVG
    this.workers = workers
    val cpuUsageAvg = cpuUsageAVG(workers)
    val memUsageAvg = memUsageAVG(workers)

    println("cpuUsageAvg" + cpuUsageAvg)
    println("memUsageAvg" + memUsageAvg)

    for (key <- workers.keys) {
      println(key)
      workerToWeight += (key -> workers(key).cpu)
    }

    for (key <- workers.keys) {
      if (workers(key).lastCpuUsage < cpuUsageAvg && workers(key).lastMemUsage < memUsageAvg) {
        workerToWeight += (key -> (workerToWeight(key) + 1))
      }
    }
    workerToWeight
  }

  def getEffect: Seq[Effect] = {
    workerToWeight.map(x => toEffect(x._1, x._2)).toSeq
  }

  def cpuUsageAVG(workers: Map[String, WorkerInfo]): Float = {
    var cpuUsageAVG = 0.0f
    workers.foreach(x => cpuUsageAVG += x._2.lastCpuUsage)
    cpuUsageAVG / workers.size
  }

  def memUsageAVG(workers: Map[String, WorkerInfo]): Float = {
    var memUsageAVG = 0.0f
    workers.foreach(x => memUsageAVG += x._2.lastMemUsage)
    memUsageAVG / workers.size
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 6) {
      println("请输入参数 host port WorkerActor的名字 serverHost serverPort MasterActor的名字")
    }

    val host = args(0)
    val port = args(1)
    val workerName = args(2)
    val serverHost = args(3)
    val serverPort = args(4)
    val masterName = args(5)

    // 创建 EvaluationActor 的 Actor 和 ActorRef
    val config = ConfigFactory.parseString(
      s"""
         |akka.actor.provider="akka.remote.RemoteActorRefProvider"
         |akka.remote.netty.tcp.hostname=$host
         |akka.remote.netty.tcp.port=$port
         |akka.actor.warn-about-java-serializer-usage=off
      """.stripMargin)

    //创建ActorSystem
    val workerActorSystem = ActorSystem("Worker", config)
    //创建WorkerActor的Actor和ActorRef
    val workerActorRef: ActorRef = workerActorSystem.actorOf(Props(new EvaluationActor(workerName, serverHost, serverPort.toInt, masterName)), s"$workerName")
    this.workerActorRef = workerActorRef
    //启动客户端
    workerActorRef ! "start"
  }
}


