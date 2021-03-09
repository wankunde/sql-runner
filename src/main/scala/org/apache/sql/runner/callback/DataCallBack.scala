// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package org.apache.sql.runner.callback

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.StructType

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2021-02-20.
 */
trait DataCallBack {

  var skipEmpty = true

  def init(schema: StructType): Unit = {}

  def next(row: GenericRowWithSchema): Unit

  def close(): Unit
}
