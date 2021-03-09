// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package org.apache.spark.sql

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.execution.command.AnalyzePartitionCommand
import org.apache.spark.sql.hive.HiveExternalCatalog.STATISTICS_NUM_ROWS
import org.apache.spark.sql.util.{DQUtil, DingTalkUtil, Logging}
import org.apache.spark.util.ThreadUtils
import org.apache.sql.runner.metrics.ReporterTrait

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}
import scala.math.BigDecimal.RoundingMode

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2020-05-06.
 */
case class InsightCatalogEventListener(implicit val spark: SparkSession)
  extends ExternalCatalogEventListener
    with ReporterTrait
    with Logging {

  implicit val catalogEventListenerContext =
    ExecutionContext.fromExecutorService(
      ThreadUtils.newDaemonCachedThreadPool("catalog-event-listener"))

  override def onEvent(event: ExternalCatalogEvent): Unit = {
    event match {
      case LoadDynamicPartitionsEvent(db, table, _, partitions, _, _) =>
        logInfo(s"LoadDynamicPartitionsEvent ${event}")
        analyzeAndCheckPartition(db, table, partitions)
      case LoadPartitionEvent(db, table, _, partition, _, _, _) =>
        logInfo(s"LoadPartitionEvent ${event}")
        analyzeAndCheckPartition(db, table, Array(partition))
      case _ =>
    }
    logDebug(s"external catalog event:${event}")
  }

  def analyzeAndCheckPartition(database: String, table: String,
                               partitionSpecs: Seq[TablePartitionSpec]): Unit = {
    val futures = partitionSpecs.map { partitionSpec =>
      Future {
        logInfo(s"load and analyze partitionSpecs : ${partitionSpec}")
        val tableIdentifier = TableIdentifier(table, Some(database))
        val partitionMap = partitionSpec.toSeq.map { case (key, value) =>
          key -> Some(value)
        }.toMap

        // there will be another AlterPartitionsEvent event.
        spark.sparkContext.setJobDescription(s"AnalyzePartition ${tableIdentifier}, partition ${partitionSpec}")
        AnalyzePartitionCommand(
          tableIdentifier,
          partitionMap,
          noscan = false).run(spark)

        val today = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"))
        if (partitionSpec.contains("dt") && !partitionSpec("dt").contains(today)) {
          // check partition values
          partitionRowNumberCheck(tableIdentifier, partitionSpec,
            SparkSession.active.sessionState.catalog.listPartitions(tableIdentifier))
        }
        SparkSession.clearActiveSession()
      }
    }

    // block util analyze partition commands ard finished.
    ThreadUtils.awaitResult(
      Future.sequence(futures),
      Duration.Inf)
  }

  def stop(): Unit = {
    catalogEventListenerContext.shutdown()
  }

  def partitionRowNumberCheck(table: TableIdentifier,
                              partitionSpec: TablePartitionSpec,
                              partitions: Seq[CatalogTablePartition],
                              samplePartitionNum: Int = 10): Unit = {
    val dt = partitionSpec("dt")

    var statsRows = -1
    val sampleRows = partitions.sortWith((p1, p2) => p1.spec("dt") > p2.spec("dt"))
      .foldLeft(ArrayBuffer[Int]()) { (ret, part) =>
        if (part.spec == partitionSpec) {
          statsRows = part.parameters.getOrElse(STATISTICS_NUM_ROWS, "-1").toInt
          reportMetrics(s"stats.counters.insight.rownums.${table.database.getOrElse("default")}.${table.table}.${dt}", statsRows)
        } else if (part.spec("dt") < dt &&
          ret.size < samplePartitionNum &&
          part.parameters.contains(STATISTICS_NUM_ROWS)) {
          ret += part.parameters(STATISTICS_NUM_ROWS).toInt
          logDebug(s"add sample partition ${part} , current sampleRows=${ret}")
        }
        ret
      }.reverse

    if (statsRows != -1 && sampleRows.size == samplePartitionNum) {
      val (ma, up, dn) = ewmaStat(sampleRows)
      logDebug(s"current sampleRows=${sampleRows}, stat = (${dn}, ${ma}, ${up})")
      if (statsRows > up || statsRows < dn) {
        val message =
          s"表${table}分区${partitionSpec}记录条数${statsRows}异常，正常区间(${dn}, ${ma}, ${up})，请关注!"
        logError(message)
        DingTalkUtil.markDownMessage(DQUtil.serverUrl, DQUtil.title, Seq(message))
      } else {
        val message = s"表${table}分区${partitionSpec}记录条数${statsRows}正常，正常区间(${dn}, ${ma}, ${up})."
        logInfo(message)
      }
    }
  }

  def bollingerStat(numRows: Seq[Int]): (BigDecimal, BigDecimal, BigDecimal) = {
    val size = numRows.size
    val ma: BigDecimal = (BigDecimal(numRows.sum) / size).setScale(2, RoundingMode.HALF_UP)
    val md = BigDecimal(math sqrt (
      numRows.map(BigDecimal(_)).fold(BigDecimal(0)) { (s, c) =>
        s + (c - ma) * (c - ma)
      } / size doubleValue
      )).setScale(2, RoundingMode.HALF_UP)

    (ma, ma + 3 * md, ma - 3 * md)
  }

  def ewmaStat(numRows: Seq[Int],
               movingVeight: BigDecimal = BigDecimal(0.8)): (BigDecimal, BigDecimal, BigDecimal) = {
    val size = numRows.length
    val predicted = numRows.tail.foldLeft(BigDecimal(numRows.head)) { (predict, value) =>
      value * movingVeight + predict * (1 - movingVeight)
    }.setScale(2, RoundingMode.HALF_UP)

    val md = BigDecimal(math sqrt (
      numRows.map(BigDecimal(_)).fold(BigDecimal(0)) { (s, c) =>
        s + (c - predicted) * (c - predicted)
      } / size doubleValue
      )).setScale(2, RoundingMode.HALF_UP)
    (predicted, predicted + 3 * md, predicted - 3 * md)
  }
}
