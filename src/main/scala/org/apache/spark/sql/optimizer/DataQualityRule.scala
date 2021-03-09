// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package org.apache.spark.sql.optimizer

import java.util.Locale

import org.apache.commons.lang3.StringUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, UnresolvedHint}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.expressions.LogicalExpressions
import org.apache.spark.util.IdGenerator
import org.apache.sql.runner.callback.{DataCallBackFactory, DataCheckCallBack}

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2021-02-20.
 */
case class DataQualityRule(spark: SparkSession) extends Rule[LogicalPlan] {

  import DataQualityRule._

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperatorsUp {
    case hint @ UnresolvedHint(hintName, parameters, child) => hintName.toUpperCase(Locale.ROOT) match {
      case "DATA_CHECK" =>
        val checkTitle: String = parameters.head.toString
        val dataCheckExpressions =
          parameters.tail map { case literal: Literal =>
            val expression = literal.toString()
            val checkResultColumn = generateDataCheckColumnName()
            val column = Column.apply(CatalystSqlParser.parseExpression(expression)).as(checkResultColumn)
            column.named.children.head.children.find { expr => child.output.contains(expr) } match {
              case Some(originColumnExpr) =>
                DataCallBackFactory.registerDataCallBack(
                  DataCheckCallBack(checkTitle,
                    child.output.find( p => p == originColumnExpr).get.name,
                    checkResultColumn,
                    expression))
                column.named

              case _ =>
                throw new RuntimeException("Data check column not matched!")
            }
          }

        Project(child.output ++ dataCheckExpressions, child)

      case _ => hint
    }
  }
}

object DataQualityRule extends Logging {
  private val ID_GENERATOR = new IdGenerator

  def generateDataCheckColumnName(): String = {
    s"__DATA_CHECK_${ID_GENERATOR.next}__"
  }
}