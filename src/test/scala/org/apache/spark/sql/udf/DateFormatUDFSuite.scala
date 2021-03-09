// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package org.apache.spark.sql.udf

import org.apache.spark.sql.{Row, SparkSqlRunnerBase}

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2020-07-20.
 */
class DateFormatUDFSuite extends SparkSqlRunnerBase {

  test("test date_format function") {
    val df = spark.sql("select transform_date('20200710','yyyyMMdd','yyyy-MM-dd')")
    checkAnswer(df, Seq(Row("2020-07-10")))
  }

}
