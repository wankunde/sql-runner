// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package org.apache.spark.sql.optimizer

import java.time.ZoneOffset

import org.apache.spark.sql.InsightSuiteUtils.cleanTestHiveData
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.hive.SparkSqlRunner
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.util.{ConfigUtil, SystemVariables}
import org.apache.sql.runner.command.SqlCommand
import org.apache.sql.runner.container.ConfigContainer
import org.scalatest.Matchers

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2020-09-15.
 */
class ExternalRelationRuleSuite extends QueryTest with SQLTestUtils with Matchers {

  override val spark = {
    System.setProperty("spark.master", "local[1]")
    SparkSqlRunner.sparkSession(Some("ExternalRelationRuleSuite"))
  }

  val testPath = getClass.getResource("/")

  val bootstrapServers = "10.23.177.40:9092"
  val schemaRegistryUrl = "http://10.23.177.40:8081"

  override def beforeAll(): Unit = {
    cleanTestHiveData()

    ConfigContainer ++ Map(
      // for SqlCommand Spark Session Name
      SystemVariables.JOB_NAME -> "ExternalRelationRuleSuite",

      // for JDBC_VIEW
      "mysql.type" -> "jdbc",
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

  test("sdf") {
    import spark.implicits._
    import scala.util.Random
    import scala.math.BigDecimal
    import java.time.LocalDateTime
    import java.time.format.DateTimeFormatter
    import java.sql.Timestamp

    spark.range(-1000, 1000).map { id =>
      val dt = Timestamp.valueOf(LocalDateTime.now.plusDays(id))
      val dt_str = LocalDateTime.now.plusDays(id).format(DateTimeFormatter.ofPattern("yyyyMMdd"))
      val dt_int = dt_str.toInt
      (id.toInt, dt, dt_str, dt_int)
    }.toDF("id", "dt", "dt_str", "dt_int")
      .createOrReplaceTempView("tab")

    spark.sql("SELECT * FROM (SELECT current_date(), id, row_number() OVER(PARTITION BY id %10 ORDER BY id ASC) rn from tab) t where rn < 5").show(false)
    spark.sql("explain extended SELECT * FROM (SELECT current_date(), id, id % 10 rn from tab) t where rn < 5").show(false)

    spark.sql(
      s"""create table dim_date (
         |    id int,
         |    dt timestamp,
         |    dt_str string,
         |    dt_int int
         |)
         |stored as parquet;
         |""".stripMargin)

    spark.sql("insert overwrite table dim_date select * from tab")

    spark.sql("select * from dim_date ").show(200, false)


    spark.sql("select * from  where dt = 20201116 limit 10").show()
    spark.sql(s"""select *
                 |from tb_bot_recommend.dws_recommend_detail t1
                 |join (
                 |   select dt_str
                 |   from dim_date
                 |   where dt_int = 20201116
                 |) t2
                 |on t1.dt = t2.dt_str
                 |limit 1000
                 |""".stripMargin).show(10, false)


    spark.range(1, 1000).map { id =>
      val id2 = id + Random.nextInt(10) - 5
      val id3 = BigDecimal((id * 100 + Random.nextInt(100)) / 100.0)
      val name = s"wankun_$id"
      val isMan = id % 2 == 0
      val birthday = Timestamp.valueOf(LocalDateTime.now.plusDays(id))
      (id.toInt, id2, id3, name, isMan, birthday)
    }.toDF("id", "id2", "id3", "name", "isMan", "birthday")
      .createOrReplaceTempView("tab2")
    spark.sql(
      s"""create table tab (
         |    id1 int,
         |    id2 bigint,
         |    id3 decimal,
         |    name string,
         |    isMan boolean,
         |    birthday timestamp
         |)
         |stored as parquet;
         |""".stripMargin)
    spark.sql("insert overwrite table tab select * from tab2")
    spark.sql("select * from tab where id1 =4 limit 10").show()
  }

  test("add external relation using hint") {

    val sqlText =
      s"""INSERT OVERWRITE TABLE target
         |SELECT /*+ JDBC_VIEW(mysql, stu) */ id, name
         |FROM stu
         |WHERE id < 100
         |""".stripMargin

    spark.sql(sqlText)
  }

  test("add external relation with query properties") {
    ConfigUtil.withConfigs("mysql.stu.query" -> "(select * from stu where name !='wankun') as q") {
      val sqlText =
        s"""SELECT /*+ JDBC_VIEW(mysql, stu) */ id, name
           |FROM stu
           |""".stripMargin

      spark.sql(sqlText).show()
    }
  }

  test("add external relation with partitionColumn properties") {
    ConfigUtil.withConfigs(
      "mysql.stu.partitionColumn" -> "id",
      "mysql.stu.lowerBound" -> "0",
      "mysql.stu.upperBound" -> "10",
      "mysql.stu.numPartitions" -> "2"
    ) {
      val sqlText =
        s"""SELECT /*+ JDBC_VIEW(mysql, stu) */ id, name
           |FROM stu
           |""".stripMargin

      val df = spark.sql(sqlText)
      df.rdd.partitions.length should equal(2)
      df.show()

      // scalastyle:off
      /**
       * Mysql LOG
       *
       * SET GLOBAL log_output = 'TABLE';SET GLOBAL general_log = 'ON';
       * SELECT * from mysql.general_log where argument like '%stu%' ORDER BY event_time DESC limit 10;
       *
       * | 2020-09-17 13:53:28.012459 | root[root] @ localhost [127.0.0.1] |      3007 |         0 | Query        | SELECT `id`,`name` FROM stu WHERE `id` >= 5                                                   |
       * | 2020-09-17 13:53:27.864487 | root[root] @ localhost [127.0.0.1] |      3006 |         0 | Query        | SELECT `id`,`name` FROM stu WHERE `id` < 5 or `id` is null                                    |
       */
      // scalastyle:on
    }
  }

  test("auto compute lowerBound, upperBound and numPartitions if absent") {
    ConfigUtil.withConfigs(
      "mysql.stu.partitionColumn" -> "id",
      "mysql.stu.numPartitions" -> "3",
    ) {
      val sqlText =
        s"""SELECT /*+ JDBC_VIEW(mysql, stu) */ id, name
           |FROM stu
           |""".stripMargin

      val df = spark.sql(sqlText)
      df.rdd.partitions.length should equal(3)
      df.show()

      // scalastyle:off
      /**
       * JOB LOG
       *
       * CREATE OR REPLACE TEMPORARY VIEW stu (`id` INT,`name` STRING,`sex` STRING,`env` STRING)
       * USING jdbc
       * OPTIONS (dbtable 'stu',numPartitions '1',upperBound '4',url 'jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=utf-8&useSSL=false&useOldAliasMetadataBehavior=true',user 'xxx',lowerBound '1',partitionColumn 'id',password 'xxx')
       */
      // scalastyle:on
    }
  }

  test("write data frame to mysql table using JDBC_SINK") {
    ConfigUtil.withConfigs(
      "mysql.queryTimeout" -> 100.toString,
      "mysql.stu.unique.keys" -> "id"
    ) {
      new SqlCommand(
        s"""WITH t as (
           |    SELECT 100 as id, "user_100" as name
           |    UNION ALL
           |    SELECT 101 as id, "user_101" as name
           |)
           |INSERT INTO stu
           |SELECT /*+ JDBC_SINK(mysql, stu) */ *
           |FROM t;
           |""".stripMargin).run()
    }
  }

  test("write data frame to mysql table alias using JDBC_SINK") {
    ConfigUtil.withConfigs(
      "mysql.queryTimeout" -> 100.toString,
      "mysql.stu2.unique.keys" -> "id"
    ) {
      new SqlCommand(
        s"""WITH t as (
           |    SELECT 100 as id, "user_100" as name
           |    UNION ALL
           |    SELECT 101 as id, "user_101" as name
           |)
           |INSERT INTO stu2
           |SELECT /*+ JDBC_SINK(mysql, stu, stu2) */ *
           |FROM t;
           |""".stripMargin).run()
    }
  }

  test("write json data frame to kafka using KAFKA_SINK") {
    ConfigUtil.withConfigs(
      "tag" -> "kafka",
      "kafka.recordType" -> "json",
      "kafka.bootstrap.servers" -> bootstrapServers,
      "kafka.stu.kafkaTopic" -> "test_wankun"
    ) {
      new SqlCommand(
        s"""WITH t as (
           |    SELECT 100 as id, "user_100" as name
           |    UNION ALL
           |    SELECT 101 as id, "user_101" as name
           |)
           |INSERT INTO stu
           |SELECT /*+ KAFKA_SINK(stu) */ *
           |FROM t;
           |""".stripMargin).run()
    }
  }

  test("write avro data frame to kafka using KAFKA_SINK") {
    ConfigUtil.withConfigs(
      "tag" -> "kafka",
      "kafka.recordType" -> "avro",
      "kafka.bootstrap.servers" -> bootstrapServers,
      "kafka.schema.registry.url" -> schemaRegistryUrl,
      "kafka.stu.kafkaTopic" -> "test_wankun2",
      "kafka.stu.avro.forceCreate" -> "true", // 根据计算结果DDL自动生成Avro Schema
      "kafka.stu.avro.name" -> "student",
      "kafka.stu.avro.namespace" -> "com.wankun"
    ) {
      new SqlCommand(
        s"""INSERT INTO stu
           |SELECT /*+ KAFKA_SINK(stu) */
           |      1 as id1, 'wankun' as name1,
           |       '男' as sex1, 'PRD' env1;
           |""".stripMargin).run()

      // 当avro Schema已经存在的时候，需要主动从avro Schema Registry上获取Schema
      ConfigContainer :+ ("kafka.stu.avro.forceCreate" -> "false")

      new SqlCommand(
        s"""INSERT INTO stu
           |SELECT /*+ KAFKA_SINK(stu) */
           |      1 as id1, 'wankun' as name1,
           |       '男' as sex1, 'PRD' env1;
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
