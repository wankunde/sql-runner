// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package org.apache.sql.runner

import java.time.LocalDateTime
import java.time.temporal.ChronoUnit

import org.apache.spark.sql.plugin.{AsyncProfilePlugin, YourkitPlugin}
import org.apache.spark.sql.util.{Logging, SystemVariables}
import org.apache.sql.runner.command.SqlCommand
import org.apache.sql.runner.container.{CollectorContainer, ConfigContainer}

import scala.reflect.io.File

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2019-12-05.
 */
object JobRunner extends ArgParser with Logging {
  def main(args: Array[String]): Unit = {
    parseArgument(args)
    logInfo(s"submit job for ${jobFile}")

    prepareRuntimeParameter()

    batchTimesOpt.getOrElse(Seq[LocalDateTime]()).map { batchTime =>
      CollectorContainer :+ (SystemVariables.BATCH_TIME -> batchTime)
      logInfo(s"submitting job(batchTime = $batchTime)")
      if (ConfigContainer.contains("dryrun")) {
        commands.foreach(_.dryrun())
      } else {
        commands.foreach(_.run())
      }
    }
    SqlCommand.stop()

    logInfo(s"end job")
  }

  def prepareRuntimeParameter(): Unit = {
    // prepare for spark mode
    val distJars = Seq("sql-runner-2.1.jar").map(jar => s"lib/${jar}").mkString(",")
    ConfigContainer :+ ("spark.yarn.dist.jars" -> distJars)
    if (!ConfigContainer.contains("spark.yarn.queue")) {
      ConfigContainer :+ ("spark.yarn.queue" -> s"root.${File(jobFile).parent.name}")
    }

    if (ConfigContainer.getOrElse("spark.profile", "false").toBoolean) {
      val profileShell = "hdfs:///deploy/config/profile.sh"
      val yourkitAgent = "hdfs:///deploy/config/libyjpagent.so"

      ConfigContainer.getOrElse("spark.profile.type", "jfr") match {
        case "yourkit" =>
          ConfigContainer :+ ("spark.profile.type" -> "snapshot")
          ConfigContainer :+ ("spark.yarn.dist.files" -> s"${profileShell},${yourkitAgent}")
          ConfigContainer :+ ("spark.yarn.dist.jars" -> s"${distJars},hdfs:///deploy/config/yjp-controller-api-redist.jar")
          ConfigContainer :+ ("spark.executor.extraJavaOptions" -> "-agentpath:libyjpagent.so=logdir=<LOG_DIR>,async_sampling_cpu")
          ConfigContainer :+ ("spark.executor.plugins" -> classOf[YourkitPlugin].getName)

        case _ =>
          ConfigContainer :+ ("spark.yarn.dist.archives" ->
            "hdfs:///deploy/config/async-profiler/async-profiler.zip#async-profiler")
          ConfigContainer :+ ("spark.yarn.dist.files" -> profileShell)
          ConfigContainer :+ ("spark.executor.extraLibraryPath" -> "./async-profiler/build/")
          ConfigContainer :+ ("spark.executor.plugins" -> classOf[AsyncProfilePlugin].getName)
      }
    }

    // 如果日期参数为空，时间设置为上一个执行周期
    if (startDate != None && endDate != None) {
      batchTimesOpt = ConfigContainer.get("period") match {
        case "minute" =>
          val rangeSize = ChronoUnit.MINUTES.between(startDate.get, endDate.get)
          Some(Range.inclusive(0, rangeSize.toInt, dateRangeStep).map(i => startDate.get.plusMinutes(i)))
        case "hour" | "hourly" =>
          val rangeSize = ChronoUnit.HOURS.between(startDate.get, endDate.get)
          Some(Range.inclusive(0, rangeSize.toInt, dateRangeStep).map(i => startDate.get.plusHours(i)))
        case "day" | "daily" =>
          val rangeSize = ChronoUnit.DAYS.between(startDate.get, endDate.get)
          Some(Range.inclusive(0, rangeSize.toInt, dateRangeStep).map(i => startDate.get.plusDays(i)))
        case "month" =>
          val rangeSize = ChronoUnit.MONTHS.between(startDate.get, endDate.get)
          Some(Range.inclusive(0, rangeSize.toInt, dateRangeStep).map(i => startDate.get.plusMonths(i)))
      }
    }
    if (batchTimesOpt == None) {
      val defaultBatchTime = {
        ConfigContainer.get("period") match {
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
  }
}
