// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package org.apache.sql.runner.config

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.util.{Logging, StringUtil, SystemVariables}
import org.apache.sql.runner.container.{CollectorContainer, ConfigContainer}
import org.apache.sql.runner.container.ConfigContainer.valueMap

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2019-12-10.
 */
class VariableSubstitution extends Logging {

  val dateTime: LocalDateTime =
    CollectorContainer.getOrElse(SystemVariables.BATCH_TIME, LocalDateTime.now)
      .asInstanceOf[LocalDateTime]

  val timeExpr = """\$\{date([+-]?)(\d*)([YMDHSymdhs]?)[|]?([[-]|[_]|[:]|[\s*]|[a-zA-Z0-9]]*)\}""".r

  /**
   *
   * @param datePattern
   * @return support pattern: yyyy-MM-dd HH:mm:ss , deault output pattern : yyyyMMdd
   */
  def dateParameter(datePattern: String): String = {
    val patternIndex = datePattern.indexOf('|')
    val newDatePattern = patternIndex match {
      case patternIndex if patternIndex > 0 =>
        datePattern.substring(0, patternIndex).replaceAll(" ", "") +
          datePattern.substring(patternIndex)
      case _ => datePattern.replaceAll(" ", "")
    }

    logDebug(s"parse pattern:$newDatePattern")
    val timeExpr(flag, window, unit, ptn) = newDatePattern
    val newDateTime = flag match {
      case "+" =>
        window match {
          case w if w.length > 0 => {
            unit match {
              case "Y" | "y" => dateTime.plusYears(w.toInt)
              case "M" => dateTime.plusMonths(w.toInt)
              case "D" | "d" => dateTime.plusDays(w.toInt)
              case "H" | "h" => dateTime.plusHours(w.toInt)
              case "m" => dateTime.plusMinutes(w.toInt)
              case "S" | "s" => dateTime.plusSeconds(w.toInt)
            }
          }
          case _ => dateTime
        }

      case "-" =>
        window match {
          case w if w.length > 0 => {
            unit match {
              case "Y" | "y" => dateTime.minusYears(w.toInt)
              case "M" => dateTime.minusMonths(w.toInt)
              case "D" | "d" => dateTime.minusDays(w.toInt)
              case "H" | "h" => dateTime.minusHours(w.toInt)
              case "m" => dateTime.minusMinutes(w.toInt)
              case "S" | "s" => dateTime.minusSeconds(w.toInt)
            }
          }
          case _ => dateTime
        }

      case _ => dateTime
    }
    val newPtn = ptn match {
      case "" => "yyyyMMdd"
      case _ => ptn
    }
    newDateTime.format(DateTimeFormatter.ofPattern(newPtn))
  }

  val dt = dateParameter("${date}")
  val yesterday = dateParameter("${date - 1d}")
  val tomorrow = dateParameter("${date + 1d}")
  val hour = dateParameter("${date |yyyyMMddHH}")
  val lastHour = dateParameter("${date - 1h |yyyyMMddHH}")
  val nextHour = dateParameter("${date + 1h |yyyyMMddHH}")

  def substitute(content: String): String = {
    var tup = doSubstitute(content)
    var retryNum = 1
    while (tup._2 && retryNum < 1000) {
      tup = doSubstitute(tup._1)
      retryNum = retryNum + 1
    }
    tup._1
  }

  def doSubstitute(content: String): (String, Boolean) = {
    val parsedSqlText = new StringBuilder()
    var flag = true
    var start = -1
    var end = -1
    var findParameter = ""
    for (pair <- content.zipWithIndex if flag) {
      if (pair._1 == '{' && pair._2 > 0 && content.charAt(pair._2 - 1) == '$') {
        start = pair._2
      } else if (start > 0 && pair._1 == '}') {
        end = pair._2
        flag = false
        findParameter = content.substring(start + 1, pair._2)
      }
    }
    if ("" != findParameter) {
      val commaPos = findParameter.indexOf(',')
      val (paramKey, defaultValue) =
        if (commaPos > 0) {
          val (keyToken, valueToken) =
            (findParameter.take(commaPos), findParameter.drop(commaPos + 1))
          (StringUtil.escapeStringValue(keyToken), Some(StringUtil.escapeStringValue(valueToken)))
        } else {
          (StringUtil.escapeStringValue(findParameter), None)
        }

      parsedSqlText.append(content.substring(0, start - 1))
      parsedSqlText.append(getParameterValue(paramKey, defaultValue))
      parsedSqlText.append(content.substring(end + 1))
      (parsedSqlText.toString(), true)
    } else {
      (content, false)
    }
  }

  private def getParameterValue(paramKey: String,
                                defaultValue: Option[String] = None): String = {
    paramKey.trim match {
      case "dt" => dt
      case "yesterday" => yesterday
      case "tomorrow" => tomorrow
      case "hour" => hour
      case "lastHour" => lastHour
      case "nextHour" => nextHour
      case datePattern: String if (paramKey.startsWith("date")) =>
        try {
          dateParameter(s"$${$datePattern}")
        } catch {
          case ex: Exception =>
            throw new Exception(s"parameter $paramKey cannot be parsed", ex)
        }
      case _ =>
        ConfigContainer.getOrElse(paramKey,
          defaultValue.getOrElse(
            throw new Exception(s"parameter $paramKey cannot be parsed")
          )
        )
    }
  }
}

object VariableSubstitution {

  def withSubstitution[T](body: VariableSubstitution => T): T = {
    val substitution = new VariableSubstitution
    val originConfigMap = valueMap.get()
    val newConfigMap = originConfigMap.map {
      case (k, v) => k -> substitution.substitute(v)
    }
    valueMap.set(newConfigMap)
    try {
      body(substitution)
    } finally {
      valueMap.set(originConfigMap)
    }
  }

}
