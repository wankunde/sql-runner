// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package org.apache.sql.runner.callback

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.util.Logging

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2021-03-08.
 */
case class SingleValueCollector(name: String, columnName: String)
  extends DataCallBack with Logging {

  var value: Any = _

  override def next(row: GenericRowWithSchema): Unit = {
    value = row.get(row.schema.fieldIndex(columnName))
  }

  override def close(): Unit = {
    CollectorContainer :+ (name, value)
  }
}
