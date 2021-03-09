// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package org.apache.spark.sql.plugin

import java.lang.management.ManagementFactory

import javax.management.ObjectName
import one.profiler.AsyncProfiler

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2020-05-14.
 */
class AsyncProfilePlugin extends ProfilePlugin {

  var profiler: AsyncProfiler = _
  
  override def init0(): Unit = {
    profileFile = s"${logDir}/${containerId}.${profileType}"

    profiler = AsyncProfiler.getInstance()
    ManagementFactory.getPlatformMBeanServer().registerMBean(
      profiler,
      new ObjectName("one.profiler:type=AsyncProfiler")
    )
    if (!manualProfile) {
      logInfo(profiler.execute(s"start,${profileType},file=${profileFile}"))
    }
  }

  override def shutdown0(): Unit = {
    logInfo(profiler.execute(s"stop,file=${profileFile}"))
  }
}
