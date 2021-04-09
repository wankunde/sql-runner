// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package org.apache.spark.sql.optimizer

import java.util.Locale

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnresolvedHint}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.util.OptimizerUtil.parseHintParameter
import org.apache.sql.runner.callback.{DataCallBackFactory, DingTalkSink, EmailSink}
import org.apache.sql.runner.container.ConfigContainer

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2020-09-15.
 */
case class ExternalSinkRule(spark: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperatorsUp {
    case hint@UnresolvedHint(hintName, parameters, child) => hintName.toUpperCase(Locale.ROOT) match {
      case "EMAIL_SINK" =>
        val name = parseHintParameter(parameters(0))
        DataCallBackFactory.registerDataCallBack(EmailSink(name, ConfigContainer.valueMap.get()))
        child

      case "DINGDING_SINK" =>
        val botName = parseHintParameter(parameters(0))
        DataCallBackFactory.registerDataCallBack(DingTalkSink(botName, ConfigContainer.valueMap.get()))
        child

      case _ => hint
    }
  }

}
