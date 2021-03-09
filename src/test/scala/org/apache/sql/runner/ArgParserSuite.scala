// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package org.apache.sql.runner

import java.time.LocalDateTime
import java.time.temporal.ChronoUnit

import JobRunner.dateRangeStep
import org.apache.spark.sql.util.Logging
import org.scalatest.{FunSuite, Matchers}

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2021-02-04.
 */
class ArgParserSuite extends FunSuite with Matchers with Logging {

  test("test time range option") {
    val startDate = Some(LocalDateTime.parse("2021-01-01T00:00:00"))
    val endDate = Some(LocalDateTime.parse("2021-01-06T00:00:00"))

    val rangeSize = ChronoUnit.DAYS.between(startDate.get, endDate.get)
    Range.inclusive(0, rangeSize.toInt, dateRangeStep).map(i => startDate.get.plusDays(i)) should
      be(Seq(LocalDateTime.parse("2021-01-01T00:00:00"),
        LocalDateTime.parse("2021-01-02T00:00:00"),
        LocalDateTime.parse("2021-01-03T00:00:00"),
        LocalDateTime.parse("2021-01-04T00:00:00"),
        LocalDateTime.parse("2021-01-05T00:00:00"),
        LocalDateTime.parse("2021-01-06T00:00:00")))


    dateRangeStep = 2
    Range.inclusive(0, rangeSize.toInt, dateRangeStep).map(i => startDate.get.plusDays(i)) should
      be(Seq(LocalDateTime.parse("2021-01-01T00:00:00"),
        LocalDateTime.parse("2021-01-03T00:00:00"),
        LocalDateTime.parse("2021-01-05T00:00:00")))

  }
}
