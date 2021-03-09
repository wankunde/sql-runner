// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package org.apache.sql.runner

import java.time.LocalDateTime
import java.time.temporal.ChronoUnit

import org.apache.spark.sql.plugin.{AsyncProfilePlugin, YourkitPlugin}
import org.apache.spark.sql.util.Logging

import scala.reflect.io.File

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2019-12-05.
 */
object JobRunner extends ArgParser with Logging {
  def main(args: Array[String]): Unit = {
    parseArgument(args)
    logInfo(s"submit job for ${jobFile}")

    // prepare for spark mode
    val distJars = Seq("above-board-2.0.jar").map(jar => s"lib/${jar}").mkString(",")
    Configuration :+ ("spark.yarn.dist.jars", distJars)
    if (!Configuration.contains("spark.yarn.queue")) {
      Configuration :+ ("spark.yarn.queue", s"root.${File(jobFile).parent.name}")
    }

    if (Configuration.getOrElse("spark.profile", "false").toBoolean) {
      val profileShell = "hdfs:///deploy/config/profile.sh"
      val yourkitAgent = "hdfs:///deploy/config/libyjpagent.so"

      Configuration.getOrElse("spark.profile.type", "jfr") match {
        case "yourkit" =>
          Configuration :+ ("spark.profile.type", "snapshot")
          Configuration :+ ("spark.yarn.dist.files", s"${profileShell},${yourkitAgent}")
          Configuration :+ ("spark.yarn.dist.jars", s"${distJars},hdfs:///deploy/config/yjp-controller-api-redist.jar")
          Configuration :+ ("spark.executor.extraJavaOptions", "-agentpath:libyjpagent.so=logdir=<LOG_DIR>,async_sampling_cpu")
          Configuration :+ ("spark.executor.plugins", classOf[YourkitPlugin].getName)

        case _ =>
          Configuration :+ ("spark.yarn.dist.archives",
            "hdfs:///deploy/config/async-profiler/async-profiler.zip#async-profiler")
          Configuration :+ ("spark.yarn.dist.files", profileShell)
          Configuration :+ ("spark.executor.extraLibraryPath", "./async-profiler/build/")
          Configuration :+ ("spark.executor.plugins", classOf[AsyncProfilePlugin].getName)
      }
    }

    // 如果日期参数为空，时间设置为上一个执行周期
    if (startDate != None && endDate != None) {
      batchTimesOpt = Configuration.get("period") match {
        case "minute" =>
          val rangeSize = ChronoUnit.MINUTES.between(startDate.get, endDate.get)
          Some(Range.inclusive(0, rangeSize.toInt, dateRangeStep).map(i => startDate.get.plusMinutes(i)))
        case "hour" =>
          val rangeSize = ChronoUnit.HOURS.between(startDate.get, endDate.get)
          Some(Range.inclusive(0, rangeSize.toInt, dateRangeStep).map(i => startDate.get.plusHours(i)))
        case "day" =>
          val rangeSize = ChronoUnit.DAYS.between(startDate.get, endDate.get)
          Some(Range.inclusive(0, rangeSize.toInt, dateRangeStep).map(i => startDate.get.plusDays(i)))
        case "month" =>
          val rangeSize = ChronoUnit.MONTHS.between(startDate.get, endDate.get)
          Some(Range.inclusive(0, rangeSize.toInt, dateRangeStep).map(i => startDate.get.plusMonths(i)))
      }
    }
    if (batchTimesOpt == None) {
      val defaultBatchTime = {
        Configuration.get("period") match {
          case "minute" =>
            val dt = LocalDateTime.now.minusMinutes(1)
            LocalDateTime.of(dt.getYear, dt.getMonth, dt.getDayOfMonth,
              dt.getHour, dt.getMinute, 0)
          case "hour" =>
            val dt = LocalDateTime.now.minusHours(1)
            LocalDateTime.of(dt.getYear, dt.getMonth, dt.getDayOfMonth, dt.getHour, 0, 0)
          case "day" =>
            val dt = LocalDateTime.now.minusDays(1)
            LocalDateTime.of(dt.getYear, dt.getMonth, dt.getDayOfMonth, 0, 0, 0)
          case "month" =>
            val dt = LocalDateTime.now.minusMonths(1)
            LocalDateTime.of(dt.getYear, dt.getMonth, 1, 0, 0, 0)
        }
      }
      batchTimesOpt = Some(Seq(defaultBatchTime))
    }

    // pull variables from apollo
    ApolloClient.pollVariablesFromApollo()

    // real job
    batchTimesOpt.getOrElse(Seq[LocalDateTime]()).map { batchTime =>
      Configuration.setBatchTime(batchTime)
      logInfo(s"submitting job(batchTime = $batchTime)")
      if (Configuration.contains("dryrun")) {
        commands.foreach(_.dryrun())
      } else {
        commands.foreach(_.run())
      }
      SqlCommand.stop()
    }
    logInfo(s"end job")
  }
}
