// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package org.apache.sql.runner.config

import java.time.LocalDateTime

import org.apache.spark.sql.util.SystemVariables
import org.apache.sql.runner.container.{CollectorContainer, ConfigContainer}
import org.scalatest.{FunSuite, Matchers}

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2019-12-10.
 */
class VariableSubstitutionSuite extends FunSuite with Matchers {

  test("test time variable") {

    CollectorContainer :+ (SystemVariables.BATCH_TIME, LocalDateTime.parse("2019-08-07T13:25:41"))
    val substitution = new VariableSubstitution()

    substitution.dateParameter("${date}") should be("20190807")
    substitution.dateParameter("${date + 2d}") should be("20190809")
    substitution.dateParameter("${date + 2d |yyyyMMddHH}") should be("2019080913")
    substitution.dateParameter("${date + 2d |yyyyMM00}") should be("20190800")
    substitution.dateParameter("${date + 2d |yyyy-MM-dd}") should be("2019-08-09")
    substitution.dateParameter("${date + 2d |yyyy_MM_dd}") should be("2019_08_09")
    substitution.dateParameter("${date-2m|yyyy-MM-dd HH:mm:ss}") should be("2019-08-07 13:23:41")

    substitution.dateParameter("${date+2d}") should be("20190809")
    substitution.dateParameter("${date+4y}") should be("20230807")

    substitution.dateParameter("${date+2D}") should be("20190809")
    substitution.dateParameter("${date+3M}") should be("20191107")
    substitution.dateParameter("${date+4Y}") should be("20230807")

    substitution.dateParameter("${date-2d}") should be("20190805")
    substitution.dateParameter("${date-4y}") should be("20150807")

    substitution.dateParameter("${date-2D}") should be("20190805")
    substitution.dateParameter("${date-3M}") should be("20190507")

    substitution.dt should be("20190807")
    substitution.yesterday should be("20190806")
    substitution.tomorrow should be("20190808")
    substitution.hour should be("2019080713")
    substitution.lastHour should be("2019080712")
    substitution.nextHour should be("2019080714")
  }

  test("test variable substitution in sql") {
    ConfigContainer :+ ("ab_target", "after_trade")
    CollectorContainer :+ (SystemVariables.BATCH_TIME, LocalDateTime.parse("2019-08-07T13:25:41"))
    val variableSubstitution = new VariableSubstitution()
    val sqlText =
      """
        |SELECT  count(1)
        |FROM    tab
        |WHERE   start_date = '${yesterday}'
        |AND     end_date = '${dt}'
        |AND     start_hour = '${date-23H|hh}'
        |AND     end_hour = '${date - 24h|hh}'
        |AND     month = '${date - 24h|MM}'
        |AND     ab_target = '${ab_target}'
        |""".stripMargin
    val newSqlText = variableSubstitution.substitute(sqlText)
    val expectSqlText =
      s"""
         |SELECT  count(1)
         |FROM    tab
         |WHERE   start_date = '20190806'
         |AND     end_date = '20190807'
         |AND     start_hour = '02'
         |AND     end_hour = '01'
         |AND     month = '08'
         |AND     ab_target = 'after_trade'
         |""".stripMargin
    newSqlText should equal(expectSqlText)
  }

  test("test nested variable substitution in sql") {
    ConfigContainer :+ ("report_days", "3")
    CollectorContainer :+ (SystemVariables.BATCH_TIME, LocalDateTime.parse("2019-08-07T13:25:41"))
    val variableSubstitution = new VariableSubstitution()
    val sqlText = "SELECT * FROM tab WHERE dt = ${date-${report_days}d|yyyyMMdd}"
    val newSqlText = variableSubstitution.substitute(sqlText)
    val expectSqlText = "SELECT * FROM tab WHERE dt = 20190804"
    newSqlText should equal(expectSqlText)
  }
}
