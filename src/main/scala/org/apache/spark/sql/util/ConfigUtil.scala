// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package org.apache.spark.sql.util

import org.apache.spark.sql.SparkSession
import org.apache.sql.runner.container.ConfigContainer

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2020-02-17.
 */
object ConfigUtil {

  def ltrim(s: String): String = s.replaceAll("^\\s+", "")

  def rtrim(s: String): String = s.replaceAll("\\s+$", "")

  def trimConfigValue(configValue: String): String = rtrim(ltrim(configValue))


  def trimConfigArray(configValue: String, separator: String): String = {
    configValue.split(separator)
      .map(trimConfigValue(_))
      .mkString(separator)
  }

  def withConfigs[T](configs: (String, String)*)(func: => T): T = {
    val spark = SparkSession.active
    try {
      configs.foreach(config => {
        ConfigContainer :+ (config._1 -> config._2)
        spark.conf.set(config._1, config._2)
      })

      func
    } finally {
      configs.foreach(config => {
        ConfigContainer - config._1
        spark.conf.unset(config._1)
      })
    }
  }
}
