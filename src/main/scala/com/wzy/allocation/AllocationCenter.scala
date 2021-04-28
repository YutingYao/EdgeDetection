package com.wzy.allocation

import com.wzy.monitor.Worker

import scala.collection.mutable.ListBuffer
import scala.util.Random

/**
 * 分区策略反馈模块
 * 根据partition信息和节点计算能力信息，使用自适应数据分区策略，对Partition数据进行合理划分
 * 基于各个节点的权重。来进行资源调度
 * 权重可理解为每个节点的命中的比例，目前权重等同于每个节点的虚拟Cpu核数
 */
object AllocationCenter {
  private var random: Random = null //前n为权重的和
  private var weightSum: Int = 0
  private var workers: Seq[Worker] = _

  /**
   * Spring Cloud Ribbon （客户端负载均衡）策略中的 WeightedResponseTimeRule
   * 此题可简述为「按权重，看作多个区间，按区间宽度越大，概率越大」
   * 在 Ribbon 相关架构中，服务端给客户端一个服务列表，类似 Map<String, Set<String>> 结构。若客户端想调用 key = serviceA，
   * 可选的具体服务端实例有 Set<String> 的 ["/svc/a1", "/svc/a2", "/svc/a3"]，由客户端自行决定
   * Ribbon 作为客户端负载均衡来帮助客户端选择去哪个具体服务实例（a1 / a2 / a3），希望雨露均沾，又希望别运气不好抽到响应慢的
   * 服务器，故有了一种根据权重的均衡策略
   * 权重是通过定时统计最近一段时间内，a1 / a2 / a3 各自的访问响应时间如何，如 a1: 10ms，a2: 20ms，a3: 40ms
   * 通过算法（不赘述，有兴趣可留言喔）计算得 a1: [0, 60]，a2: (60, 110]，a3: (110, 140] 的区间对应
   * 下次再需要访问 serviceA 时，随机一个数 [0, 140]，看落在哪个区间，就选那个实例
   * RabbitMQ 的 Topic 交换器使用 Trie 匹配
   * MySQL 中的 IN 语法涉及二分算法
   *
   * @param partitionsIndex
   * @param workers
   * @return
   */
  def distrbutionByWeight(partitionsIndex: Seq[Int], workers: Seq[Worker]): Map[Int, Seq[String]] = {

    this.workers = workers
    val psum = new ListBuffer[Int]
    workers.foreach(x => {
      weightSum += x.totalCores
      psum += weightSum
    })


    var indexToPrefs: Map[Int, Seq[String]] = Map()

    random = new Random()

    for (i <- partitionsIndex) {
      val index = pickIndex(psum)
      val workerName = workers(index).hostPort.split(":")(0)
      indexToPrefs += (i -> Seq(workerName))
    }

    indexToPrefs

  }

  def pickIndex(psum: Seq[Int]): Int = {
    val targ = random.nextInt(weightSum)
    var lo = 0
    var hi = psum.size - 1
    while (lo != hi) {
      val mid: Int = (lo + hi) / 2
      if (targ >= psum(mid)) lo = mid + 1
      else hi = mid
    }
    lo
  }
}
