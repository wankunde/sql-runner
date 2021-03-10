// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package org.apache.sql.runner.callback

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.util.Logging
import org.apache.sql.runner.container.CollectorContainer

import scala.collection.mutable.ArrayBuffer

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2021-03-08.
 */
case class ArrayValueCollector(name: String, columnName: String)
  extends DataCallBack with Logging {

  val array = ArrayBuffer[Any]()

  override def next(row: GenericRowWithSchema): Unit = {
    array += row.get(row.schema.fieldIndex(columnName))
  }

  override def close(): Unit = {
    CollectorContainer + (name -> array.toArray)
  }
}
