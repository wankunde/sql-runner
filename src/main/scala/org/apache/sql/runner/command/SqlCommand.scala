// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package org.apache.sql.runner.command

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.SparkSqlRunner
import org.apache.spark.sql.util.{Logging, SystemVariables}
import org.apache.sql.runner.callback.DataCallBackFactory
import org.apache.sql.runner.config.VariableSubstitution
import org.apache.sql.runner.container.ConfigContainer

import scala.collection.JavaConverters._

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2021-02-24.
 */
case class SqlCommand(sourceChars: SourceChars)
  extends BaseCommand(sourceChars) {

  def this(sourceString: String) {
    this(SourceChars(sourceString.toCharArray, 0, sourceString.length))
  }

  sourceChars.start = sourceChars.start + CommandFactory.sqlPrefix.length

  val (sql, _, nextStart) = readTo(";")
  sourceChars.start = nextStart

  override def toString: String = s"$sql;"

  override def run(): Unit = {
    doRun(isDryRun = false)
  }

  override def dryrun(): Unit = {
    doRun(isDryRun = true)
  }

  def doRun(isDryRun: Boolean): Unit = {
    VariableSubstitution.withSubstitution { substitution =>
      // 这里需要注意参数的还原
      val sqlText = substitution.substitute(sql)
      logInfo(s"sql content:\n$sqlText")
      if (!isDryRun) {
        DataCallBackFactory.consumeResult(SqlCommand.sparkSqlRunner.run(sqlText))
      }
    }
  }
}

object SqlCommand extends Logging {

  implicit lazy val sparkSession: SparkSession =
    SparkSqlRunner.sparkSession(
      Some(ConfigContainer.getOrElse(SystemVariables.JOB_NAME, "Unknown Job Name")))

  lazy val sparkSqlRunner = new SparkSqlRunner

  //  val catalogEventListener = InsightCatalogEventListener()
  var sqlContext = sparkSession.sqlContext

  //  SparkSession.active.sharedState.externalCatalog.addListener(catalogEventListener)

  /** Cleans up and shuts down the Spark SQL environments. */
  def stop() {
    logDebug("Clear SparkSession and SparkContext")
    // TODO
    //    catalogEventListener.stop()
    if (sqlContext != null) {
      sqlContext = null
    }
    if (sparkSession != null) {
      sparkSession.stop()
    }
    SparkSession.clearActiveSession

    val clazz = Class.forName("java.lang.ApplicationShutdownHooks")
    val field = clazz.getDeclaredField("hooks")
    field.setAccessible(true)
    val inheritableThreadLocalsField = classOf[Thread].getDeclaredField("inheritableThreadLocals")
    inheritableThreadLocalsField.setAccessible(true)

    val hooks = field.get(clazz).asInstanceOf[java.util.IdentityHashMap[Thread, Thread]].asScala
    hooks.keys.map(inheritableThreadLocalsField.set(_, null))
  }

  def simpleTypeName(typeName: String): String = {
    val i = typeName.indexOf("(")
    if (i > 0) {
      typeName.substring(0, i)
    } else {
      typeName
    }
  }
}
