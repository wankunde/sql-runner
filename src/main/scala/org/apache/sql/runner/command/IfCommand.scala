// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package org.apache.sql.runner.command

import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Cast, Literal}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.types.DataType
import org.apache.sql.runner.config.VariableSubstitution
import org.apache.sql.runner.container.{CollectorContainer, ConfigContainer}

import scala.collection.mutable
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
      val dataTypeMap = mutable.Map[String, DataType]()

      val originExpr = CatalystSqlParser.parseExpression(substitution.substitute(ifConditionString))

      var lastMapSize = -1
      while (lastMapSize != dataTypeMap.size) {
        lastMapSize = dataTypeMap.size
        originExpr transform {
          case expr: BinaryExpression =>
            (expr.left, expr.right) match {
              case (attr: UnresolvedAttribute, literal: Literal) =>
                dataTypeMap += (attr.name -> literal.dataType)

              case (literal: Literal, attr: UnresolvedAttribute) =>
                dataTypeMap += (attr.name -> literal.dataType)

              case (attr1: UnresolvedAttribute, attr2: UnresolvedAttribute) =>
                if (dataTypeMap.contains(attr1.name)) {
                  dataTypeMap += (attr2.name -> dataTypeMap(attr1.name))
                }
                if (dataTypeMap.contains(attr2.name)) {
                  dataTypeMap += (attr1.name -> dataTypeMap(attr2.name))
                }

              case (_, _) =>
            }
            expr

          case e => e
        }
      }

      val ifCondition =
        originExpr transform {
          case e: UnresolvedAttribute =>
            val dataType = dataTypeMap(e.name)
            val literal = Literal(CollectorContainer.getOrElse(e.name, ConfigContainer.get(e.name)))
            if (dataType == literal.dataType) {
              literal
            } else {
              Cast(literal, dataType)
            }

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


