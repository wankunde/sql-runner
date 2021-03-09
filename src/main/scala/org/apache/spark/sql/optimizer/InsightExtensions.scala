// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package org.apache.spark.sql.optimizer

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSessionExtensions

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2020-04-17.
 */
class InsightExtensions extends (SparkSessionExtensions => Unit) with Logging {
  def apply(e: SparkSessionExtensions): Unit = {
    e.injectOptimizerRule(RepartitionRule)
  }
}
