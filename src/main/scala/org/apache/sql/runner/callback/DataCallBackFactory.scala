// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package org.apache.sql.runner.callback

import org.apache.spark.sql.util.Logging

import scala.collection.mutable.ArrayBuffer

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2021-03-08.
 */
object DataCallBackFactory extends Logging {

  val callBacks: ThreadLocal[ArrayBuffer[DataCallBack]] =
    new ThreadLocal[ArrayBuffer[DataCallBack]] {
      override def initialValue(): ArrayBuffer[DataCallBack] = ArrayBuffer[DataCallBack]()
    }

  def registerDataCallBack(dataCallBack: DataCallBack): Unit = {
    logInfo(s"add new data call back:\n$dataCallBack")
    callBacks.get() += dataCallBack
  }

  def clearDataCallBack(): Unit = callBacks.get().clear()

  def consumeResult(qr: QueryResult): Unit = {
    val iterator = qr.iterator
    while (iterator.hasNext) {
      iterator.next()
    }
  }
}