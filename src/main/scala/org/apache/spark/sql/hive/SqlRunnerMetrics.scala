// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package org.apache.spark.sql.hive

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.hive.execution.{HiveTableScanExec, InsertIntoHiveTable}

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2020-04-29.
 */
object SqlRunnerMetrics extends Logging {

  def logSparkPlanMetrics(plan: SparkPlan): Unit = plan match {
    case HiveTableScanExec(_, relation, _) =>
      logInfo(s"source ${relation.nodeName}(${relation.tableMeta.identifier}) metrics : ${formatMetrics(plan.metrics)}")
    case DataWritingCommandExec(cmd: InsertIntoHiveTable, _) =>
      logInfo(s"Insert table ${cmd.table.identifier} metrics : ${formatMetrics(plan.metrics)}")

    case _ =>
  }

  def formatMetrics(metrics: Map[String, SQLMetric]): Map[String, Long] = metrics.map {
    case (name: String, metric: SQLMetric) =>
      name -> metric.value
  }
}
