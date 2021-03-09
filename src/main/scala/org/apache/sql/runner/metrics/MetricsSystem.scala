// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package org.apache.sql.runner.metrics

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2020-02-26.
 */
trait MetricsSystem {

  lazy val reporter: Option[GraphiteReporter] = {
    val enableMetrics = Configuration.getOrElse("metrics.enable", "true").toBoolean
    if (enableMetrics && Configuration.contains("graphite.host")) {
      val graphiteHost = Configuration.get("graphite.host")
      val graphitePort = Configuration.getOrElse("graphite.port", "2003").toInt
      Some(GraphiteReporter(graphiteHost, graphitePort))
    } else {
      None
    }
  }

  def reportMetrics(key: String, value: Number): Unit =
    reporter.map(_.reportMetrics(key, value))
}
