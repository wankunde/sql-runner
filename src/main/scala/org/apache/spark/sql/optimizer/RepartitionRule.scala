// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package org.apache.spark.sql.optimizer

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import org.apache.hadoop.hive.common.StatsSetupConst
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTablePartition}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Repartition, RepartitionByExpression, _}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand
import org.apache.spark.sql.hive.execution.InsertIntoHiveTable
import org.apache.spark.sql.util.SystemVariables.INDEX_COLUMN_NAME

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2020-04-17.
 */
case class RepartitionRule(spark: SparkSession) extends Rule[LogicalPlan] with Logging {

  val DEFAULT_PARTITION_SIZE = 64 * 1024 * 1024L
  val SAMPLING_PARTITIONS = 10

  val analyzer = spark.sessionState.analyzer
  val catalog = SparkSession.active.sessionState.catalog

  override def apply(plan: LogicalPlan): LogicalPlan = {
    val newPlan = plan transform {
      case InsertIntoHiveTable(table, partition, query, overwrite, partitionExists, outputCols)
        if table.partitionColumnNames.size > 0 && checkQueryType(query) =>

        val newQuery: LogicalPlan = transformQuery(table, query)
        InsertIntoHiveTable(table, partition, newQuery, overwrite, partitionExists, outputCols)

      case InsertIntoHadoopFsRelationCommand(outputPath, staticPartitions, ifPartitionNotExists,
      partitionColumns, bucketSpec, fileFormat, options, query, mode, catalogTable, fileIndex,
      outputColumnNames)
        if catalogTable.isDefined && (staticPartitions.size + partitionColumns.size) > 0
          && checkQueryType(query) =>
        val newQuery =
          transformQuery(catalogTable.get, query)

        InsertIntoHadoopFsRelationCommand(
          outputPath,
          staticPartitions,
          ifPartitionNotExists,
          partitionColumns,
          bucketSpec,
          fileFormat,
          options,
          newQuery,
          mode,
          catalogTable,
          fileIndex,
          outputColumnNames)
    }
    if (!newPlan.fastEquals(plan)) {
      logDebug(s"plan after RepartitionRule:\n$newPlan")
    }
    newPlan
  }

  private def checkQueryType(query: LogicalPlan): Boolean = {
    !query.isInstanceOf[Sort] && !query.isInstanceOf[Repartition] &&
      !query.isInstanceOf[RepartitionByExpression]
  }

  private def transformQuery(table: CatalogTable, query: LogicalPlan): LogicalPlan = {
    val tableName = table.identifier
    val sortExprsOpt: Option[Seq[SortOrder]] =
      table.properties.get(INDEX_COLUMN_NAME).map(indexColumn => {
        val order = Symbol(indexColumn).attr.asc
        Seq(analyzer.resolveExpressionBottomUp(order, query).asInstanceOf[SortOrder])
      })

    val numPartitionsOpt = repartitionNumbers(catalog.listPartitions(tableName))
    (sortExprsOpt, numPartitionsOpt) match {
      case (Some(sortExprs), Some(numPartitions)) =>
        RepartitionByExpression(sortExprs, query, numPartitions)

      case (Some(sortExprs), None) => Sort(sortExprs, true, query)
      case (None, Some(numPartitions)) => Repartition(numPartitions, true, query)
      case (None, None) => query
    }
  }

  /**
   * 1. 根据分区创建时间倒排序，取最近创建的分区
   * 2. sample 采样10个分区元数据来计算分区个数，取结果中位数
   * @param partitions
   * @return
   */
  def repartitionNumbers(partitions: Seq[CatalogTablePartition]): Option[Int] = {

    val stats = new DescriptiveStatistics
    if (log.isDebugEnabled) {
      partitions.foreach(p => logDebug(s"got partition ${p.simpleString}"))
    }
    partitions.filter(_.parameters.contains(StatsSetupConst.TOTAL_SIZE))
      .sortWith((p1, p2) => p1.createTime > p2.createTime)
      .slice(0, SAMPLING_PARTITIONS)
      .foreach { p =>
        stats.addValue(p.parameters.get(StatsSetupConst.TOTAL_SIZE).get.toLong
          / DEFAULT_PARTITION_SIZE)
      }
    if (stats.getPercentile(50).isNaN) {
      None
    } else {
      val number = stats.getPercentile(50).toInt + 1
      if (number > 0) {
        Some(number)
      } else {
        None
      }
    }
  }
}
