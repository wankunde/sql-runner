// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package org.apache.spark.sql.optimizer

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.hive.SparkSqlRunner
import org.apache.spark.sql.test.SQLTestData.TestData
import org.apache.spark.sql.test.SQLTestUtils
import org.scalatest.Matchers

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2020-07-28.
 */
class CollectValueRuleSuite extends QueryTest with SQLTestUtils with Matchers {

  override val spark = {
    System.setProperty("spark.master", "local[1]")
    SparkSqlRunner.sparkSession(Some("CollectValueRuleSuite"))
  }

  import spark.implicits._

  override def beforeAll() {
    val df = spark.sparkContext.parallelize(
      (1 to 100).map(i => TestData(i, i.toString))).toDF()
    df.createOrReplaceTempView("testData")
  }

  override def afterAll(): Unit = {
    spark.close()
  }


  def runPartitionScanLimitRule(testQuery: String): Unit = {
    PartitionScanLimitRule(spark).apply(
      spark.sql(testQuery).queryExecution.optimizedPlan
    )
  }

  def runAndComsume(sql: String): Unit = {
    DataCallBackFactory.consumeResult(SqlCommand.sparkSqlRunner.run(sql))
  }

  test("test collect Hint") {
    runAndComsume(
      s"""SELECT /*+ COLLECT_VALUE('single_value', 'count_column') */
         |       /*+ COLLECT_VALUE('max_key', 'keyColumn') */
         |       count(1) as count_column,
         |       concat('prefix_', max(key)) as keyColumn
         |from  testData
         |""".stripMargin)
    CollectorContainer.get("single_value") should be(100)
    CollectorContainer.get("max_key") should be("prefix_100")

    runAndComsume(
      s"""SELECT /*+ COLLECT_ARRAY('intArray', 'key') */
         |       /*+ COLLECT_ARRAY('stringArray', 'value') */
         |       key, value
         |from  testData
         |""".stripMargin)
    CollectorContainer.get("intArray") should be((1 to 100))
    CollectorContainer.get("stringArray") should be((1 to 100).map(_.toString))
  }
}
