// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package org.apache.spark.sql.plugin

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2020-05-14.
 */
class YourkitPlugin extends ProfilePlugin {

  override def shutdown0(): Unit = {
    val controllerCls = Class.forName("com.yourkit.api.Controller")
    val controller = controllerCls.newInstance()

    val displayNameMethod = controllerCls.getMethod("capturePerformanceSnapshot")
    profileFile = displayNameMethod.invoke(controller).asInstanceOf[String]

    val stopCpuProfilingMethod = controllerCls.getMethod("stopCpuProfiling")
    stopCpuProfilingMethod.invoke(controller)
  }
}
