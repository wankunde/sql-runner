// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package org.apache.spark.sql.optimizer

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, HiveTableRelation}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.{AnalysisException, SparkSession}

import scala.collection.mutable.ArrayBuffer

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2020-07-28.
 */
case class PartitionScanLimitRule(spark: SparkSession) extends Rule[LogicalPlan] with Logging {

  val partitionScanLimitEnable: Boolean =
    spark.conf.get("spark.partition.scan.limit.enable", "true").toBoolean

  def conditionCheck(partitionColNames: Seq[String],
                     filters: ArrayBuffer[Expression],
                     tableMeta: CatalogTable): Unit = {
    val filteredAttributes = filters.flatMap(_.references.map(_.name.toLowerCase))
    if ((partitionColNames.map(_.toLowerCase) intersect filteredAttributes).size == 0) {
      val table = tableMeta.identifier
      throw new AnalysisException(
        s"""Does not find partition column filter condition for table $table
           |partitionColNames : ${partitionColNames.mkString(", ")}
           |filteredAttributes : $filteredAttributes
           |""".stripMargin)
    }
  }

  def checkRelationFilters(plan: LogicalPlan, filters: ArrayBuffer[Expression]): Unit =
    plan match {
      case Filter(condition, child) if condition.deterministic =>
        checkRelationFilters(child, filters :+ condition)

      case HiveTableRelation(catalogTable, _, partitionCols, _, _)
        if partitionCols.nonEmpty =>
        val partitionColNames = partitionCols.map(_.name)
        conditionCheck(partitionColNames, filters, catalogTable)

      case LogicalRelation(relation: HadoopFsRelation, _, catalogTableOpt, _) =>
        relation.partitionSchemaOption.map { case partitionSchema =>
          val partitionColNames = partitionSchema.fieldNames
          conditionCheck(partitionColNames, filters, catalogTableOpt.get)
        }

      case Join(left, right, _, _, _) =>
        checkRelationFilters(left, ArrayBuffer[Expression]())
        checkRelationFilters(right, ArrayBuffer[Expression]())

      case _ =>
        plan.children.map(checkRelationFilters(_, filters))
    }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (partitionScanLimitEnable) {
      checkRelationFilters(plan, ArrayBuffer[Expression]())
    }
    plan
  }
}
