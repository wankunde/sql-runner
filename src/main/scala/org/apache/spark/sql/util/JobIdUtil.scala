// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package org.apache.spark.sql.util

import java.io.File
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2020-03-06.
 */
object JobIdUtil {

  def generatorJobId(jobFile: String): String = {
    val ts = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"))
    val prefix = new File(jobFile).getName.stripSuffix(".xml")
    s"${prefix}-${ts}"
  }
}
