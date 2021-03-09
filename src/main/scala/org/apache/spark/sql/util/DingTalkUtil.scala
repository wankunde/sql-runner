// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package org.apache.spark.sql.util

import com.dingtalk.api.DefaultDingTalkClient
import com.dingtalk.api.request.OapiRobotSendRequest

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2020-02-13.
 */
object DingTalkUtil extends Logging {
  def markDownMessage(serverUrl: String,
                      title: String,
                      messages: Seq[String]): Unit = {
    val client = new DefaultDingTalkClient(serverUrl)

    val request = new OapiRobotSendRequest
    request.setMsgtype("markdown")
    val markdown = new OapiRobotSendRequest.Markdown
    markdown.setTitle(title)
    markdown.setText(
      s"""
         |# $title

         |${messages.mkString("\n\n")}
             """.stripMargin)
    request.setMarkdown(markdown)
    val response = client.execute(request)
    val responseMessage = new StringBuilder
    if (response.getCode != null) {
      responseMessage.append(s"code: ${response.getCode}\t")
    }
    if (response.getMessage != null) {
      responseMessage.append(s"message: ${response.getMessage}\t")
    }
    if (response.getErrcode != null) {
      responseMessage.append(s"errCode: ${response.getErrcode}\t")
    }
    if (response.getErrmsg != null) {
      responseMessage.append(s"errmsg: ${response.getErrmsg}\t")
    }
    if (response.getSubCode != null) {
      responseMessage.append(s"subCode: ${response.getSubCode}\t")
    }
    if (response.getSubMessage != null) {
      responseMessage.append(s"subMessage: ${response.getSubMessage}\t")
    }

    logInfo(s"DingTalk respose: ${responseMessage}")
  }
}
