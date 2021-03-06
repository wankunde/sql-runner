// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package org.apache.sql.runner

import org.apache.spark.sql.util.{DQUtil, DingTalkUtil, SystemVariables}
import org.apache.sql.runner.container.ConfigContainer

/**
 * 对非测试，运行失败的程序进行告警
 *
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2020-02-26.
 */
object Alert extends ArgParser {
  def main(args: Array[String]): Unit = {
    if (!args.contains("--test") && !args.contains("--dryrun")) {
      parseArgument(args)
      val env = ConfigContainer.getOrElse(SystemVariables.ENV, SystemVariables.DEFAULT_ENV)

      val alertMessage = s"$env : 程序 ${args(0)} 运行失败，请检查！"
      DingTalkUtil.markDownMessage(DQUtil.serverUrl, DQUtil.title, Seq(alertMessage))
    }
  }
}
