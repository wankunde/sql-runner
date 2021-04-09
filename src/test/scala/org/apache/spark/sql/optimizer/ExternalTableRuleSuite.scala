// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package org.apache.spark.sql.optimizer

import org.apache.spark.sql.InsightSuiteUtils.cleanTestHiveData
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.hive.SparkSqlRunner
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.util.ConfigUtil
import org.apache.sql.runner.command.SqlCommand
import org.apache.sql.runner.container.ConfigContainer
import org.scalatest.Matchers

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2020-09-15.
 */
class ExternalTableRuleSuite extends QueryTest with SQLTestUtils with Matchers {

  override val spark = {
    System.setProperty("spark.master", "local[1]")
    SparkSqlRunner.sparkSession(Some("ExternalTableRuleSuite"))
  }

  val testPath = getClass.getResource("/")

  val bootstrapServers = "10.23.177.40:9092"
  val schemaRegistryUrl = "http://10.23.177.40:8081"

  override def beforeAll(): Unit = {
    cleanTestHiveData()

    ConfigContainer ++ Map(
      "mysql.url" -> "jdbc:mysql://localhost:3306/test",
      "mysql.username" -> "root",
      "mysql.password" -> "password",
    )

    spark.sql(s"CREATE TABLE target(id int, name string) LOCATION '$testPath/target'")

    /**
     * mysql> desc stu;
     * +-------+------------+------+-----+---------+-------+
     * | Field | Type       | Null | Key | Default | Extra |
     * +-------+------------+------+-----+---------+-------+
     * | id    | int(11)    | NO   | PRI | NULL    |       |
     * | name  | text       | YES  |     | NULL    |       |
     * | sex   | varchar(2) | YES  |     | NULL    |       |
     * | env   | char(20)   | YES  |     | NULL    |       |
     * +-------+------------+------+-----+---------+-------+
     */
  }

  override def afterAll() {
    cleanTestHiveData()
    spark.stop()
    super.afterAll()
  }

  test("query jdbc table") {
    ConfigUtil.withConfigs(
      "mysql.stu.numPartitions" -> "3",
      "mysql.stu.partitionColumn" -> "id") {

      val df = spark.sql(
        s"""SELECT id, name
           |FROM jdbc.mysql.stu
           |""".stripMargin)
      df.rdd.partitions.length should equal(3)
      df.show()
    }
  }

  test("query jdbc view") {
    ConfigUtil.withConfigs(
      "mysql.stu.query" -> "(select * from stu where name !='wankun') as q",
      "mysql.stu.numPartitions" -> "3",
      "mysql.stu.partitionColumn" -> "id") {

      val df = spark.sql(
        s"""SELECT id, name
           |FROM jdbc.mysql.stu
           |""".stripMargin)
      df.rdd.partitions.length should equal(3)
      df.show()
    }
  }

  test("write data frame to mysql table") {
    ConfigUtil.withConfigs(
      "mysql.stu.queryTimeout" -> 100.toString,
      "mysql.stu.uniqueKeys" -> "id"
    ) {
      new SqlCommand(
        s"""WITH t as (
           |    SELECT 100 as id, "user_100" as name
           |    UNION ALL
           |    SELECT 101 as id, "user_101" as name
           |)
           |INSERT INTO jdbc.mysql.stu
           |SELECT *
           |FROM t;
           |""".stripMargin).run()
    }
  }

  test("write json data frame to kafka table") {
    ConfigUtil.withConfigs(
      "kafka.bootstrap.servers" -> bootstrapServers,
      "kafka.stu.recordType" -> "json",
      "kafka.stu.kafkaTopic" -> "test_wankun"
    ) {
      new SqlCommand(
        s"""WITH t as (
           |    SELECT 100 as id, "user_100" as name
           |    UNION ALL
           |    SELECT 101 as id, "user_101" as name
           |)
           |INSERT INTO kafka.stu
           |SELECT *
           |FROM t;
           |""".stripMargin).run()
    }
  }

  test("write avro data frame to kafka using KAFKA_SINK") {
    ConfigUtil.withConfigs(
      "kafka.bootstrap.servers" -> bootstrapServers,
      "kafka.schema.registry.url" -> schemaRegistryUrl,
      "kafka.stu.recordType" -> "avro",
      "kafka.stu.kafkaTopic" -> "test_wankun2",
      // 不根据计算结果DDL自动生成Avro Schema，手动测试时，根据需要调整该参数
      "kafka.stu.avro.forceCreate" -> "false",
      "kafka.stu.avro.name" -> "student",
      "kafka.stu.avro.namespace" -> "com.wankun"
    ) {
      new SqlCommand(
        s"""INSERT INTO kafka.stu
           |SELECT 1 as id1, 'wankun' as name1,
           |       '男' as sex1, 'PRD' env1, 18 age1;
           |""".stripMargin).run()
    }
  }

  /*
  test("send message with EMAIL_SINK") {
    ConfigUtil.withConfigs(
      // server config
      "email.hostname" -> "smtp.exmail.qq.com",
      "email.username" -> "test@leyantech.com",
      "email.password" -> "",
      "email.from" -> "test@leyantech.com",

      // job config
      "email.columns" -> "id, name",
      "email.columnNames" -> "ID,名称",
      "email.subject" -> "测试邮件",
      "email.email-to" -> "kun.wan@leyantech.com",
      "email.email-cc" -> "kun.wan@leyantech.com"
    ) {
      new SqlCommand(
        s"""SELECT /*+ EMAIL_SINK(email) */
           |      1 as id, 'wankun' as name;
           |""".stripMargin).run()
    }
  }

  test("send message with DINGDING_SINK") {
    ConfigUtil.withConfigs(
      "dataquality.alert"-> "https://oapi.dingtalk.com/robot/send?access_token=test_token",
      "dataquality.alert.title" -> "测试钉钉告警",
      "dataquality.alert.pattern" -> "ID是{id}，姓名:{name}"
    ) {
      new SqlCommand(
        s"""SELECT /*+ DINGDING_SINK(dataquality.alert) */
           |      1 as id, 'wankun' as name;
           |""".stripMargin).run()
    }
  }
  */

}
