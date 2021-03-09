// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package org.apache.spark.sql.util

import org.scalatest.{FunSuite, Matchers}

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2020-03-06.
 */
class JobIdUtilSuite extends FunSuite with Matchers {

  test("test generatorJobId") {
    val jobId = JobIdUtil.generatorJobId("conf/marketing/pdd/dwd_payment_reminder_detail.xml")
    jobId should fullyMatch regex ("""dwd_payment_reminder_detail-\d{8}_\d{6}""")
  }

}
