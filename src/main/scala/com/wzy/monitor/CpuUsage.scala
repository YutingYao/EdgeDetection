package com.wzy.monitor

import java.io.BufferedReader
import java.io.IOException
import java.io.InputStreamReader
import java.io.PrintWriter
import java.io.StringWriter
import scala.util.control._

import org.apache.log4j.Logger


/**
 * 采集CPU使用率
 */
object CpuUsage {
  private val log = Logger.getLogger(classOf[CpuUsage])
  private val INSTANCE = new CpuUsage

  def getInstance: CpuUsage = INSTANCE

  /**
   * @param args
   * @throws InterruptedException
   */
  @throws[InterruptedException]
  def main(args: Array[String]): Unit = {
    while ( {
      true
    }) {
      System.out.println(CpuUsage.getInstance.get)
      Thread.sleep(5000)
    }
  }
}

class CpuUsage {
  /**
   * Purpose:采集CPU使用率
   *
   * @return float,CPU使用率,小于1
   */
  def get: Float = {
    CpuUsage.log.info("开始收集cpu使用率")
    var cpuUsage: Float = 0
    val r = Runtime.getRuntime
    try {
      val command = "cat /proc/stat"
      //第一次采集CPU时间
      val startTime = System.currentTimeMillis
      val pro1: Process = r.exec(command)
      val in1 = new BufferedReader(new InputStreamReader(pro1.getInputStream))
      var line: String = in1.readLine
      var idleCpuTime1: Long = 0
      var totalCpuTime1: Long = 0 //分别为系统启动后空闲的CPU时间和总的CPU时间

      val loop = new Breaks
      loop.breakable {
        while (line != null) {
          if (line.startsWith("cpu")) {
            line = line.trim
            CpuUsage.log.info(line)
            val temp: Array[String] = line.split("\\s+")
            idleCpuTime1 = temp(4).toLong
            for (s <- temp) {
              if (!(s == "cpu")) totalCpuTime1 += s.toLong
            }
            CpuUsage.log.info("IdleCpuTime: " + idleCpuTime1 + ", " + "TotalCpuTime" + totalCpuTime1)
            loop.break
          }
          line = in1.readLine
        }
      }
      in1.close()
      pro1.destroy()
      try Thread.sleep(100)
      catch {
        case e: InterruptedException =>
          val sw = new StringWriter
          e.printStackTrace(new PrintWriter(sw))
          CpuUsage.log.error("CpuUsage休眠时发生InterruptedException. " + e.getMessage)
          CpuUsage.log.error(sw.toString)
      }
      //第二次采集CPU时间
      val endTime = System.currentTimeMillis
      val pro2: Process = r.exec(command)
      val in2 = new BufferedReader(new InputStreamReader(pro2.getInputStream))
      var idleCpuTime2: Long = 0
      var totalCpuTime2: Long = 0 //分别为系统启动后空闲的CPU时间和总的CPU时间
      line = in2.readLine

      loop.breakable {
        while (line != null) {
          if (line.startsWith("cpu")) {
            line = line.trim
            CpuUsage.log.info(line)
            val temp: Array[String] = line.split("\\s+")
            idleCpuTime2 = temp(4).toLong
            for (s <- temp) {
              if (!(s == "cpu")) totalCpuTime2 += s.toLong
            }
            CpuUsage.log.info("IdleCpuTime: " + idleCpuTime2 + ", " + "TotalCpuTime" + totalCpuTime2)
            loop.break
          }
          line = in2.readLine
        }
      }
      in1.close()
      if (idleCpuTime1 != 0 && totalCpuTime1 != 0 && idleCpuTime2 != 0 && totalCpuTime2 != 0) {
        cpuUsage = 1 - (idleCpuTime2 - idleCpuTime1).toFloat / (totalCpuTime2 - totalCpuTime1).toFloat
        CpuUsage.log.info("本节点CPU使用率为: " + cpuUsage)
      }
      in2.close()
      pro2.destroy()
    } catch {
      case e: IOException =>
        val sw = new StringWriter
        e.printStackTrace(new PrintWriter(sw))
        CpuUsage.log.error("CpuUsage发生InstantiationException. " + e.getMessage)
        CpuUsage.log.error(sw.toString)
    }
    cpuUsage
  }
}