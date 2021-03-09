// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package org.apache.sql.runner

import java.time.LocalDateTime

import org.apache.commons.io.FilenameUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.util.SystemVariables

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2020-06-03.
 */
class ArgParser {

  var batchTimesOpt: Option[Seq[LocalDateTime]] = None
  var startDate: Option[LocalDateTime] = None
  var endDate: Option[LocalDateTime] = None
  var dateRangeStep: Int = 1
  var jobFile: String = _
  var commands: Array[BaseCommand] = _

  def parseArgument(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("env and job configuration file must be found!")
      System.exit(-1)
    }

    val leftArgs = new ArrayBuffer[String]()
    var argv = args.toList


    while (!argv.isEmpty) {
      argv match {
        case "--dateRange" :: startDateStr :: endDateStr :: tail =>
          startDate = Some(LocalDateTime.parse(startDateStr))
          endDate = Some(LocalDateTime.parse(endDateStr))
          argv = tail
        case "--dates" :: dates :: tail =>
          batchTimesOpt = Some(dates.split(",").map(LocalDateTime.parse(_)).toSeq)
          argv = tail
        case "--config" :: value :: tail =>
          val tup = value.split("=")
          Configuration :+ (tup(0), tup(1))
          argv = tail
        case "--profile" :: tail =>
          Configuration :+ ("spark.profile", "true")
          argv = tail
        case "--dryrun" :: tail =>
          Configuration :+ ("dryrun", "true")
          argv = tail
        case "--dateRangeStep" :: dateRangeStepStr :: tail =>
          dateRangeStep = dateRangeStepStr.toInt
          argv = tail
        case head :: tail if head != null =>
          leftArgs.append(head)
          argv = tail
      }
    }

    jobFile = leftArgs(0)

    Configuration :+ (SystemVariables.JOB_NAME, FilenameUtils.getBaseName(jobFile))
    if (StringUtils.isNotBlank(System.getenv(SystemVariables.ENV))) {
      Configuration :+ (SystemVariables.ENV, System.getenv(SystemVariables.ENV))
    }
    if (StringUtils.isNotBlank(System.getenv(SystemVariables.APOLLO_META))) {
      Configuration :+ (SystemVariables.APOLLO_META, System.getenv(SystemVariables.APOLLO_META))
    }

    commands = CommandFactory.parseCommands(Source.fromFile(jobFile).mkString)
    assert(commands.length > 0 && commands(0).isInstanceOf[BlockCommentCommand])
    checkHeader(commands(0).asInstanceOf[BlockCommentCommand])
  }

  def checkHeader(cmd: BlockCommentCommand): Unit = {
    val keys = Set("author", "period", "run_env", "describe")
    val headerMap: Map[String, String] =
      cmd.comment.split('\n')
        .filter(_.contains(":"))
        .map { line =>
          val splits = line.split(":")
          splits(0).trim -> splits(1).trim
        }.toMap

    val notExistsKeys = keys.filterNot(headerMap.contains(_))
    assert(notExistsKeys.isEmpty, s"Header 中缺少 ${notExistsKeys.mkString(", ")} 参数!")
    for ((key, value) <- headerMap) {
      Configuration.:+(key, value)
    }
  }
}
