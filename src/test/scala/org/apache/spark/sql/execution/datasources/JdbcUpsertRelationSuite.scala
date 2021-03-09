// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.SparkSqlRunnerBase

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2020-06-02.
 */
class JdbcUpsertRelationSuite extends SparkSqlRunnerBase {

  val url = "jdbc:mysql://localhost:3306/test?useUnicode=true&amp;characterEncoding=utf-8&amp;useSSL=false&amp;useOldAliasMetadataBehavior=true"
  val username = "root"
  val passoword = "password"

  test("test upsert mysql table") {
    spark.sql(
      s"""
         |CREATE OR REPLACE TEMPORARY VIEW stu (id int, name string, sex string)
         |USING org.apache.spark.sql.execution.datasources.JdbcUpsertRelationProvider
         |OPTIONS (url '$url', dbtable 'test.stu', user '$username', password '$passoword', unique.keys 'id,name')
       """.stripMargin)

    spark.sql(
      s"""
         |INSERT INTO stu
         |SELECT 1 as id1, 'wankun' as name1, '男' as sex1
         |""".stripMargin
    )
  }

  test("test upsert mysql table with format alias") {
    spark.sql(
      s"""
         |CREATE OR REPLACE TEMPORARY VIEW stu (id int, name string, sex string)
         |USING mysql_upsert_sink
         |OPTIONS (url '$url', dbtable 'test.stu', user '$username', password '$passoword', unique.keys 'id,name')
       """.stripMargin)

    spark.sql(
      s"""
         |INSERT INTO stu
         |SELECT 1 as id1, 'wankun' as name1, '男' as sex1
         |""".stripMargin
    )
  }
}
