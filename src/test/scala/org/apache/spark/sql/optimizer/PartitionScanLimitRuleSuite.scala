// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package org.apache.spark.sql.optimizer

import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.{AnalysisException, QueryTest}

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2020-07-28.
 */
class PartitionScanLimitRuleSuite extends QueryTest with SQLTestUtils with TestHiveSingleton {

  override def beforeAll(): Unit = {
    super.beforeAll()

    Seq("test1", "test2").map { tableName =>
      sql(
        s"""
           |CREATE TABLE $tableName(i int)
           |PARTITIONED BY (p STRING)
           |STORED AS textfile""".stripMargin)
      sql(s"INSERT OVERWRITE TABLE $tableName PARTITION (p='1') select * from range(10)")
    }
  }

  override def afterAll(): Unit = {
    Seq("test1", "test2").map { tableName =>
      sql(s"DROP TABLE IF EXISTS $tableName")
    }
    super.afterAll()
  }

  def runPartitionScanLimitRule(testQuery: String): Unit = {
    PartitionScanLimitRule(spark).apply(
      spark.sql(testQuery).queryExecution.optimizedPlan
    )
  }

  test("no filters on partition table scan") {
    intercept[AnalysisException] {
      runPartitionScanLimitRule("SELECT i FROM test1")
    }

    runPartitionScanLimitRule("SELECT i FROM test1 where p='1'")
    runPartitionScanLimitRule(
      s"""
         |WITH t as (
         |    SELECT count(1) as c
         |    FROM test1
         |    WHERE p='1'
         |)
         |SELECT * FROM t
         |""".stripMargin)
  }

  test("no filters on partition table join") {
    intercept[AnalysisException] {
      runPartitionScanLimitRule(
        s"""
           |SELECT *
           |FROM (SELECT i FROM test1 where p='1') t1
           |JOIN test2 t2
           |ON t1.i > t2.i
           |""".stripMargin)
    }

    runPartitionScanLimitRule(
      s"""
         |SELECT *
         |FROM (SELECT i FROM test1 where p='1') t1
         |JOIN test2 t2
         |ON t1.i > t2.i
         |AND t2.p = '1'
         |""".stripMargin)
  }
}
