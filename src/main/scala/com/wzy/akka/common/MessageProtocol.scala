package com.wzy.akka.common

//使用样例类来构建协议

//Worker注册信息
case class RegisterWorkerInfo(id: String, CpuCores: Int, ram: Long)

//  这个是WorkerInfo，是保存在Master的HashMap中的，该HashMap用于管理Worker
// 将来这个WorkerInfo会扩展，比如增加Worker上一次的心跳时间
@SerialVersionUID(123L)
class WorkerInfo(val id: String, val cpu: Int, val ram: Long) extends Serializable {
  // 新增属性：心跳时间
  var lastHeartBeatTime: Long = _

  // CPU使用率
  var lastCpuUsage: Float = _

  // 内存使用率
  var lastMemUsage: Float = _
}

// 当Worker注册成功，服务器返回一个RegisteredWorkerInfo对象
case object RegisteredWorkerInfo

// 每隔一定时间定时器发送给 Master 一个心跳
case class HeartBeat(id: String, cpuUsage: Float, memUsage: Float)

// Worker每隔一定时间定时器发送给自己一个消息
case object SendHeartBeat

// Master给自己发送一个触发检查超时Worker的信息
case object StartTimeOutWorker

// Master给自己发消息，检测Worker，对于心跳超时的
case object RemoveTimeOutWorker

// Evaluationzhu
case object RegisterEvaluation
// Evaluation获取worker信息
case object GetMasterActorWorkers
