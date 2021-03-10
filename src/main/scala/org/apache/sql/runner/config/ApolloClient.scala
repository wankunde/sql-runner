// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package org.apache.sql.runner.config

import com.ctrip.framework.apollo.{Config, ConfigService}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.util.{Logging, SystemVariables}
import org.apache.sql.runner.container.ConfigContainer

import scala.collection.JavaConverters._

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2020-03-04.
 */
case class ApolloClient(namespace: String) extends Logging {

  lazy val config: Config = ConfigService.getConfig(namespace)

  def getProperty(key: String, defaultValue: String): String = {
    config.getProperty(key, defaultValue)
  }
}

object ApolloClient extends Logging {

  /**
   * 去Apollo 获取参数太慢了
   *
   * @return
   */
  def pollVariablesFromApollo(): Unit = {
    if (StringUtils.isNotBlank(System.getenv(SystemVariables.APOLLO_META))) {
      val appId =
        ConfigContainer.getOrElse("apollo.app.id",
          ConfigContainer.getOrElse("appId",
            SystemVariables.DEFAULT_APOLLO_ID))
      System.setProperty("app.id", appId)

      val systemClient = ApolloClient("1.above-board")

      systemClient.config.getPropertyNames
        .toArray.map { case key: String =>
        val value = systemClient.getProperty(key, "")
        val encryptedValue = if (key.toLowerCase.contains("password")) "******" else value
        logInfo(s"pull variable from apollo, $key = $encryptedValue)")
        ConfigContainer :+ (key -> value)
      }

      if (ConfigContainer.contains("apollo.namespace")) {
        val appClient = ApolloClient(ConfigContainer.get("apollo.namespace"))
        appClient.config.getPropertyNames.asScala.map { case key: String =>
          val value = appClient.getProperty(key, "")
          val encryptedValue = if (key.toLowerCase.contains("password")) "******" else value
          logInfo(s"pull variable from apollo, $key = $encryptedValue")
          ConfigContainer :+ (key -> value)
        }
      }
    }
  }
}