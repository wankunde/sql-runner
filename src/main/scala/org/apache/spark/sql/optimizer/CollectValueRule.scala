// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package org.apache.spark.sql.optimizer

import java.util.Locale

import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnresolvedHint}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.util.OptimizerUtil.parseHintParameter
import org.apache.sql.runner.callback.{ArrayValueCollector, DataCallBackFactory, SingleValueCollector}

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2020-09-15.
 */
object CollectValueRule extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperatorsUp {
    case hint@UnresolvedHint(hintName, parameters, child) => hintName.toUpperCase(Locale.ROOT) match {
      case "COLLECT_VALUE" =>
        val name: String = parseHintParameter(parameters(0))
        val columnName: String = parseHintParameter(parameters(1))
        DataCallBackFactory.registerDataCallBack(SingleValueCollector(name, columnName))

        child

      case "COLLECT_ARRAY" =>
        val name: String = parseHintParameter(parameters(0))
        val columnName: String = parseHintParameter(parameters(1))
        DataCallBackFactory.registerDataCallBack(ArrayValueCollector(name, columnName))

        child

      case _ => hint
    }
  }
}
