// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package org.apache.sql.runner.command

import scala.collection.mutable.ArrayBuffer

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2021-02-24.
 */
object CommandFactory {
  val sqlPrefix = ""
  val lineCommentPrefix = "--"
  val blockCommentPrefix = "/**"
  val setPrefix = "!set"

  val ifPrefix = "!if"
  val elsePrefix = "!else"
  val fiPrefix = "!fi"

  def skipEmptyChars(sourceChars: SourceChars): Unit = {
    while (sourceChars.start < sourceChars.chars.length &&
      Character.isWhitespace(sourceChars.chars.charAt(sourceChars.start))) {
      sourceChars.start = sourceChars.start + 1
    }
  }

  /**
   * 使用探测法，找到下一条Command
   * @param sourceChars
   */
  def nextCommand(sourceChars: SourceChars): BaseCommand = {
    skipEmptyChars(sourceChars)
    val commandPrefix: Option[String] =
      Seq(
        lineCommentPrefix,
        blockCommentPrefix,
        setPrefix,
        ifPrefix,
        elsePrefix,
        fiPrefix
      ) find { prefix =>
        val len = prefix.length
        if (sourceChars.start + len >= sourceChars.end) {
          false
        }
        else {
          prefix.equalsIgnoreCase(new String(sourceChars.chars, sourceChars.start, len))
        }
      }

    val cmd =
      commandPrefix match {
        case Some(prefix) if prefix == lineCommentPrefix => LineCommentCommand(sourceChars)
        case Some(prefix) if prefix == blockCommentPrefix => BlockCommentCommand(sourceChars)
        case Some(prefix) if prefix == setPrefix => SetCommand(sourceChars)
        case Some(prefix) if prefix == ifPrefix => IfCommand(sourceChars)
        case Some(prefix) if prefix == elsePrefix => ElseCommand(sourceChars)
        case Some(prefix) if prefix == fiPrefix => FiCommand(sourceChars)
        case None => SqlCommand(sourceChars)
      }
    skipEmptyChars(sourceChars)
    cmd
  }

  def parseCommands(source: String): Array[BaseCommand] = {
    val commands = ArrayBuffer[BaseCommand]()
    val sourceChars = SourceChars(source.toCharArray, 0, source.length)

    while (sourceChars.start < source.length) {
      val command = nextCommand(sourceChars)
      commands += command
    }
    commands.toArray
  }
}
