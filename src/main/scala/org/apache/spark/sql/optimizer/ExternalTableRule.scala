// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package org.apache.spark.sql.optimizer

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.{AnalysisContext, UnresolvedRelation}
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoStatement, LogicalPlan, With}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.QueryExecution
import org.apache.sql.runner.container.ConfigContainer

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2021-04-07.
 */
case class ExternalTableRule(spark: SparkSession) extends Rule[LogicalPlan] {

  import spark.sessionState.analyzer._

  // from Analyzer
  private def isResolvingView: Boolean = AnalysisContext.get.catalogAndNamespace.nonEmpty

  // If we are resolving relations insides views, we need to expand single-part relation names with
  // the current catalog and namespace of when the view was created.
  private def expandRelationName(nameParts: Seq[String]): Seq[String] = {
    if (!isResolvingView) return nameParts

    if (nameParts.length == 1) {
      AnalysisContext.get.catalogAndNamespace :+ nameParts.head
    } else if (spark.sessionState.catalogManager.isCatalogRegistered(nameParts.head)) {
      nameParts
    } else {
      AnalysisContext.get.catalogAndNamespace.head +: nameParts
    }
  }

  def setSchemaDDL(u: UnresolvedRelation, child: LogicalPlan): Unit = {
    expandRelationName(u.multipartIdentifier) match {
      case NonSessionCatalogAndIdentifier(catalog, ident) =>
        val schemaDDL = new QueryExecution(spark, child).analyzed.schema.toDDL
        ConfigContainer :+ (s"${ident.toString}.schemaDDL" -> schemaDDL)

      case _ =>
    }
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan match {
      case InsertIntoStatement(u: UnresolvedRelation, _, query: LogicalPlan, _, _) =>
        setSchemaDDL(u, query)

      case With(InsertIntoStatement(u: UnresolvedRelation, _, query: LogicalPlan, _, _), cteRelations) =>
        setSchemaDDL(u, With(query, cteRelations))

      case _ =>
    }
    plan
  }
}
