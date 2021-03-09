// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package org.apache.spark.sql.udf

import java.time.format.DateTimeFormatter
import java.time.format.DateTimeFormatter._

import org.sparkproject.guava.cache.CacheLoader
import org.sparkproject.guava.cache.CacheBuilder

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2020-07-20.
 */
object DateFormatUDF {

  lazy val cache = CacheBuilder.newBuilder()
    .maximumSize(100)
    .build(new CacheLoader[String, DateTimeFormatter] {
      override def load(key: String): DateTimeFormatter = ofPattern(key)
    })

  implicit def toFormatter(pattern: String): DateTimeFormatter = cache.get(pattern)

  // function name : transform_date
  val transform_date_udf: (String, String, String) => String = {
    (dt: String, srcPattern: String, dstPattern: String) =>
      toFormatter(dstPattern).format(srcPattern.parse(dt))
  }
}
