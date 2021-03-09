// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package org.apache.spark.sql.util

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2020-02-26.
 */
object DQUtil {

  val serverUrl = Configuration.getOrElse("dataquality.alert", "")
  val title = s"${Configuration.getOrElse("env", "UNKNOWN")}数据质量检查告警"
}
