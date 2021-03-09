// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package org.apache.sql.runner.callback

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.util.{DQUtil, DingTalkUtil, Logging}

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2021-03-08.
 */
case class DataCheckCallBack(title: String,
                             originColumn: String,
                             checkResultColumn: String,
                             expression: String)
  extends DataCallBack with Logging {

  override def next(row: GenericRowWithSchema): Unit = {
    val value: Any = row.get(row.schema.fieldIndex(originColumn))
    val checkResult: Boolean = row.getAs(checkResultColumn)
    val messages =
      Seq(title,
        s"数据检查${if (checkResult) "正常" else "异常"}",
        s"检查条件: $expression",
        s"实际值 $value ${if (!checkResult) "不" else ""}满足条件!")

    logInfo(messages.mkString("\n"))
    if (!checkResult) {
      DingTalkUtil.markDownMessage(DQUtil.serverUrl, DQUtil.title, messages)
    }
  }

  override def close(): Unit = {}
}