// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package org.apache.spark.sql.plugin

import java.util.{Map => JMap}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.deploy.yarn.YarnSparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.util.SignalUtils
import org.apache.spark.SparkConf
import org.apache.spark.api.plugin.{ExecutorPlugin, PluginContext}

import scala.reflect.io.File


/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2020-05-26.
 */
abstract class ProfilePlugin extends ExecutorPlugin with Logging {

  val pluginName = this.getClass.getName.stripSuffix("$")

  var conf: SparkConf = _
  var manualProfile: Boolean = _
  var profileType: String = _

  val logDir = System.getProperty("spark.yarn.app.container.log.dir")
  val containerId = YarnSparkHadoopUtil.getContainerId
  val applicationAttemptId = containerId.getApplicationAttemptId
  val applicationId = applicationAttemptId.getApplicationId

  var profileFile: String = _

  val fs = FileSystem.get(new Configuration())
  var shutdownFlag = false

  def init0(): Unit = {}

  def shutdown0(): Unit = {}

  override def init(ctx: PluginContext, extraConf: JMap[String, String]): Unit = {
    conf = ctx.conf()
    manualProfile = conf.getBoolean("spark.profile.manualprofile", false)
    profileType = conf.get("spark.profile.type", "jfr")

    init0()
    logInfo(s"init ProfileExecutorPlugin")

    // Handle SIGTERM from NodeManager
    Seq("TERM", "HUP", "INT").foreach { sig =>
      SignalUtils.register(sig) {
        log.error("Executor RECEIVED SIGNAL " + sig)
        while(!shutdownFlag) {
          Thread sleep 100
          log.error("Executor shutdown loopback. SIGNAL " + sig)
        }
        log.error("ProfilePlugin Shutdown loop end. SIGNAL " + sig)
        false
      }
    }
  }

  /**
   * 1. Shutdown method is already a ShutdownHook.
   * 2. Executor may be killed by NodeManager before the shutdown method is finished.
   *  The default wait time  is 250ms defined by sleepDelayBeforeSigKill in ContainerLaunch Service.
   */
  override def shutdown(): Unit = {
    if (!manualProfile) {
      logInfo(s"shutdown ${pluginName}")
      shutdown0()

      logInfo("begin upload executor profile file.")

      val srcPath = new Path(profileFile)
      val dstPath = new Path(s"/metadata/logs/profile/${applicationId}/" +
        s"${applicationAttemptId.getAttemptId}/${containerId}.${profileType}")
      logInfo(s"profileFile :${srcPath} hdfs path : ${dstPath}")
      fs.copyFromLocalFile(true, true, srcPath, dstPath)
      File(profileFile).delete()
    }
    logInfo(s"end ${pluginName}")
    shutdownFlag = true
  }
}
