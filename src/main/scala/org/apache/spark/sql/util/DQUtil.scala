// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package org.apache.spark.sql.util

import org.apache.sql.runner.container.ConfigContainer

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2020-02-26.
 */
object DQUtil {

  val serverUrl = ConfigContainer.getOrElse("dataquality.alert", "")
  val title = s"${ConfigContainer.getOrElse(SystemVariables.ENV, SystemVariables.DEFAULT_ENV)}数据质量检查告警"
}
