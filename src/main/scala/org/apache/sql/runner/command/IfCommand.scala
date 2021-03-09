// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package org.apache.sql.runner.command

import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.sql.runner.config.VariableSubstitution
import org.apache.sql.runner.container.{CollectorContainer, ConfigContainer}

import scala.collection.mutable.ArrayBuffer

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2021-02-24.
 */
case class IfCommand(sourceChars: SourceChars)
  extends BaseCommand(sourceChars) {

  def this(sourceString: String) {
    this(SourceChars(sourceString.toCharArray, 0, sourceString.length))
  }

  sourceChars.start = sourceChars.start + CommandFactory.ifPrefix.length

  val (_, _, nextStart1) = readTo("(")
  sourceChars.start = nextStart1
  val (ifConditionString, _, nextStart2) = readTo(")")
  sourceChars.start = nextStart2

  val ifCommands = new ArrayBuffer[BaseCommand]()
  val elseCommands = new ArrayBuffer[BaseCommand]()

  var parseStage = "if"
  while (parseStage != "fi") {
    val cmd = CommandFactory.nextCommand(sourceChars)
    cmd match {
      case _: FiCommand =>
        parseStage = "fi"

      case _: ElseCommand =>
        parseStage = "else"

      case _ =>
        parseStage match {
          case "if" =>
            ifCommands += cmd
          case "else" =>
            elseCommands += cmd
        }
    }
  }

  override def toString: String = {
    val elseString =
      if (elseCommands.size > 0) {
        s"""\n!else
           |${elseCommands.mkString("\n")}
           |""".stripMargin
      } else {
        ""
      }

    s"""!if ($ifConditionString)
       |${ifCommands.mkString("\n") + elseString}
       |!fi
       |""".stripMargin

  }

  override def run(): Unit = {
    doRun(isDryRun = false)
  }

  override def dryrun(): Unit = {
    doRun(isDryRun = true)
  }

  def doRun(isDryRun: Boolean): Unit = {
    VariableSubstitution.withSubstitution { substitution =>
      val ifCondition =
        CatalystSqlParser.parseExpression(substitution.substitute(ifConditionString)) transform {
          case e: UnresolvedAttribute =>
            Literal(
              CollectorContainer.getOrElse(e.name, ConfigContainer.get(e.name))
            )

          case e => e
        }

      val ret = ifCondition.eval().asInstanceOf[Boolean]
      if (ret) {
        ifCommands.foreach(cmd => if (isDryRun) cmd.run() else cmd.dryrun())
      } else {
        elseCommands.foreach(cmd => if (isDryRun) cmd.run() else cmd.dryrun())
      }
    }

  }
}


