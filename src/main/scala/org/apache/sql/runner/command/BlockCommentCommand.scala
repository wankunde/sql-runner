// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package org.apache.sql.runner.command

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2021-02-24.
 */
case class BlockCommentCommand(sourceChars: SourceChars)
  extends BaseCommand(sourceChars) {

  def this(sourceString: String) {
    this(SourceChars(sourceString.toCharArray, 0, sourceString.length))
  }

  sourceChars.start = sourceChars.start + CommandFactory.blockCommentPrefix.length

  val (comment, _, nextStart) = readTo("*/")
  sourceChars.start = nextStart

  override def toString: String = s"/**${comment}*/"

  override def run(): Unit = {
    logInfo(s"\n${this.toString}")
  }
}
