// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package org.apache.sql.runner.metrics

import java.io.PrintWriter
import java.net.Socket

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2020-02-26.
 */
case class GraphiteReporter(host: String, port: Int) extends AutoCloseable with Serializable {

  @transient val socket: Socket = new Socket(host, port)
  @transient val out: PrintWriter = new PrintWriter(socket.getOutputStream, true)

  def reportMetrics(key: String, value: Number): Unit = {
    val timestamp = System.currentTimeMillis() / 1000
    out.printf(s"${key} ${value} ${timestamp}%n")
  }

  override def close(): Unit = {
    if (out != null) {
      out.close()
    }
    if (socket != null) {
      socket.close()
    }
  }
}
