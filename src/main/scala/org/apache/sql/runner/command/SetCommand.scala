// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package org.apache.sql.runner.command

import org.apache.sql.runner.config.VariableSubstitution
import org.apache.sql.runner.container.ConfigContainer

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2021-02-24.
 */
case class SetCommand(sourceChars: SourceChars) extends BaseCommand(sourceChars) {

  def this(sourceString: String) {
    this(SourceChars(sourceString.toCharArray, 0, sourceString.length))
  }

  sourceChars.start = sourceChars.start + CommandFactory.setPrefix.length

  val (key, _, valueStart) = readTo('=')
  sourceChars.start = valueStart

  val (value, _, nextStart) = readTo(';')
  sourceChars.start = nextStart

  override def toString: String = s"${CommandFactory.setPrefix} $key = $value;"

  override def run(): Unit = {
    val substitutionValue =
      VariableSubstitution.withSubstitution { substitution =>
        substitution.substitute(value)
      }

    ConfigContainer :+ (key -> substitutionValue)
    logInfo(s"\n${this.toString}")
  }
}