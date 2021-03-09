// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package org.apache.sql.runner.metrics

import org.apache.sql.runner.container.ConfigContainer

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2020-02-26.
 */
trait ReporterTrait {

  lazy val reporter: Option[GraphiteReporter] = {
    val enableMetrics = ConfigContainer.getOrElse("metrics.enable", "true").toBoolean
    if (enableMetrics && ConfigContainer.contains("graphite.host")) {
      val graphiteHost = ConfigContainer.get("graphite.host")
      val graphitePort = ConfigContainer.getOrElse("graphite.port", "2003").toInt
      Some(GraphiteReporter(graphiteHost, graphitePort))
    } else {
      None
    }
  }

  def reportMetrics(key: String, value: Number): Unit =
    reporter.map(_.reportMetrics(key, value))
}
