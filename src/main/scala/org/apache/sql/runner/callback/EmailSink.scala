// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package org.apache.sql.runner.callback

import java.util.Properties

import javax.activation.DataHandler
import javax.mail.internet.{InternetAddress, MimeBodyPart, MimeMessage, MimeMultipart}
import javax.mail.{Message, Session}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.util.ConfigUtil

import scala.collection.mutable.ArrayBuffer

case class EmailSink(name: String, config: Map[String, String]) extends Sink {

  // email邮件服务器参数
  val hostName = config.getOrElse(
    "email.hostname",
    throw new IllegalArgumentException("config email.hostname is needed.")
  )
  val userName = config.getOrElse(
    "email.username",
    throw new IllegalArgumentException("config email.username is needed.")
  )
  val password = config.getOrElse(
    "email.password",
    throw new IllegalArgumentException("config email.password is needed.")
  )
  val from = config.getOrElse(
    "email.from",
    throw new IllegalArgumentException("config email.from is needed.")
  )

  // email内容构建参数
  val names = ConfigUtil.trimConfigArray(
    config.getOrElse(
      s"$name.columns",
      throw new IllegalArgumentException(s"config $name.columns is needed.")
    ),
    ","
  )
  val columnNames = ConfigUtil.trimConfigArray(
    config.getOrElse(
      s"$name.columnNames",
      throw new IllegalArgumentException(s"config $name.columnNames is needed.")
    ),
    ","
  )
  val to = config.getOrElse(
    s"$name.email-to",
    throw new IllegalArgumentException(s"config $name.email-to is needed.")
  )

  val cc = config.getOrElse(s"$name.email-cc", "")

  val emailPattern = EmailSink.generateTitle(names)
  val emailColumnName = EmailSink.generateTitle(columnNames)
  val emailTemplate = config.getOrElse(
    s"$name.email-template",
    s"""<table border="1"><tbody><tr>%s</tbody></table>"""
  )
  val csvPattern = columnNames
  val subject = envName + "环境:" + config.getOrElse(s"$name.subject", "no subject")
  val attachedFileName = config.getOrElse("email-attach-filename", subject)

  val emailContent = new ArrayBuffer[String]()
  val csvContentBuffer = new ArrayBuffer[String]()
  emailContent.append(emailColumnName)
  csvContentBuffer.append(columnNames)

  var i = 0

  override def next(row: GenericRowWithSchema): Unit = {
    if (i < rowLimit) {
      emailContent.append(parsePattern(emailPattern, row))
      i = i + 1
    }
    csvContentBuffer.append(parsePattern(names, row))

  }

  override def close(): Unit = {
    val htmlContent = emailTemplate.format(emailContent.mkString("\n"))
    val csvContent = csvContentBuffer.mkString("\n")

    // 邮件发送
    val properties = new Properties()
    properties.put("mail.transport.protocol", "smtp")
    properties.put("mail.smtp.host", hostName)
    properties.put("mail.smtp.port", "465")
    properties.put(
      "mail.smtp.socketFactory.class",
      "javax.net.ssl.SSLSocketFactory"
    )
    properties.put("mail.smtp.auth", "true")
    properties.put("mail.smtp.ssl.enable", "true")

    val session = Session.getInstance(properties)
    val message = new MimeMessage(session)
    message.setFrom(new InternetAddress(from, userName))
    message.addRecipients(Message.RecipientType.TO, to)
    message.addRecipients(Message.RecipientType.CC, cc)
    message.setSubject(subject)
    val multipart = new MimeMultipart()
    val contentPart = new MimeBodyPart()
    contentPart.setContent(htmlContent, "text/html;charset=UTF-8")
    multipart.addBodyPart(contentPart)
    val mdp = new MimeBodyPart()
    val dh = new DataHandler(
      new String(Array[Byte](0xEF.toByte, 0xBB.toByte, 0xBF.toByte)) + csvContent,
      "text/plain;charset=UTF-8"
    )
    mdp.setFileName(attachedFileName + ".csv")
    mdp.setDataHandler(dh)
    multipart.addBodyPart(mdp)
    message.setContent(multipart)
    val transport = session.getTransport
    transport.connect(from, password)
    transport.sendMessage(message, message.getAllRecipients)
    transport.close
    logInfo(s"Email sink finished")
  }

  override def toString: String = {
    s"EmailSink(name = $name, from = $from, to = $to, cc = $cc, " +
      s"names = $names, columnNames = $columnNames)"
  }

}

object EmailSink {
  def generateTitle(columnName: String): String = {
    val columnTitle = columnName.split(",")
      .map(col => s"<td align='center'>${ConfigUtil.trimConfigValue(col)}</td>")
      .mkString

    s"<tr>${columnTitle}</tr>"
  }
}
