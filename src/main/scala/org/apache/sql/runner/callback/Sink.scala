// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package org.apache.sql.runner.callback

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.util.{Logging, SystemVariables}
import org.apache.sql.runner.metrics.ReporterTrait

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2019-12-12.
 */
trait Sink extends DataCallBack with ReporterTrait with Logging {

  val config: Map[String, String]

  val envName = config.getOrElse(SystemVariables.ENV, "UNKNOWN")

  var resultRows: Long = 0

  val defaultRowLimit: String = "1000"

  val rowLimit: Int = config.getOrElse("rowLimit", defaultRowLimit).toInt

  def parsePattern(pattern: String, row: GenericRowWithSchema): String = {
    val sb = new StringBuilder
    var startIdx = -1
    for ((c, idx) <- pattern.zipWithIndex) {
      if (c == '{' && startIdx < 0) {
        startIdx = idx
      } else if (c == '}' && startIdx >= 0) {
        val variableName = pattern.substring(startIdx + 1, idx)
        val fieldValue: AnyRef = row.getAs(variableName)
        sb.append(fieldValue)
        startIdx = -1
      } else if (startIdx < 0) {
        sb.append(c)
      }
    }

    sb.toString
  }
}
