// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package org.apache.sql.runner.callback

import com.taobao.api.ApiException
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.util.DingTalkUtil

import scala.collection.mutable.ArrayBuffer

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2019-12-06.
 */
case class DingTalkSink(botName: String, config: Map[String, String]) extends Sink {

  val serverUrl: String = config.getOrElse(
    botName,
    throw new IllegalArgumentException(s"config ${botName} is needed.")
  )

  val pattern: String = config(s"${botName}.pattern")
  val title: String = s"${envName}数据监控告警 - " + config(s"${botName}.title")

  var i = 0
  val resultBuffer = new ArrayBuffer[String]()

  override def next(row: GenericRowWithSchema): Unit = {
    if (i < rowLimit) {
      resultBuffer.append(parsePattern(pattern, row))
      i = i + 1
    }
  }

  @throws[ApiException]
  override def close(): Unit = {
    DingTalkUtil.markDownMessage(serverUrl, title, resultBuffer)
  }

  override def toString: String = {
    s"DingTalkSink(botName = $botName, title = $title)"
  }

}
