// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package org.apache.spark.sql.execution.datasources

import java.util
import java.util.Properties

import io.confluent.kafka.serializers.{AbstractKafkaAvroSerDeConfig, KafkaAvroSerializer}
import org.apache.avro.Schema
import org.apache.avro.Schema._
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericData.Record
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.sql.SparkSqlRunnerBase
import org.apache.spark.sql.types.StructType
import org.scalatest.Matchers

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2020-07-13.
 */
class KafkaSinkRelationSuite extends SparkSqlRunnerBase with Matchers {


  val bootstrapServers = "10.23.177.40:9092"
  val schemaRegistryUrl = "http://10.23.177.40:8081"

  test("ensure the columns's order in avro schema is the same as dataframe schema") {
    val dfSchema = StructType.fromDDL("id int, is_student boolean, name string")
    val kafkaOptions = KafkaOptions("", Map("tag" -> "kafka",
      "recordType" -> "avro",
      "kafka.bootstrap.servers" -> "$bootstrapServers",
      "kafka.schema.registry.url" -> "$schemaRegistryUrl",
      "kafkaTopic" -> "test_wankun",
      "avro.name" -> "student",
      "avro.namespace" -> "org.apache"
    ))
    val relation = KafkaSinkRelation(dfSchema, kafkaOptions)(spark)


    val fields = util.Arrays.asList(
      new Field("id", create(Type.INT), "", null),
      new Field("is_student", create(Type.BOOLEAN), "", null),
      new Field("name", create(Type.STRING), "", null),
    )
    val targetSchema =
      createRecord(
        "student",
        "test custom schema",
        "org.apache",
        false,
        fields)

    relation.avroSchema should equal(targetSchema)
  }

  test("send json message to kafka") {
    spark.sql(
      s"""
         |CREATE OR REPLACE TEMPORARY VIEW stu (id1 int, name1 string, sex1 string, env1 string)
         |USING org.apache.spark.sql.execution.datasources.KafkaSinkRelationProvider
         |OPTIONS (
         |  tag 'kafka',
         |  recordType 'json',
         |  kafka.bootstrap.servers '$bootstrapServers',
         |  kafkaTopic 'test_wankun'
         |)
       """.stripMargin)

    spark.sql(
      s"""
         |INSERT INTO stu
         |SELECT 1 as id1, 'wankun' as name1,
         |       '男' as sex1, 'PRD' env1
         |""".stripMargin)

    // Check for result
    // kafka-console-consumer --bootstrap-server 10.23.177.40:9092   --topic test_wankun
    // {"id1":1,"name1":"wankun","sex1":"男","env1":"PRD"}
  }

  /**
   * kafka topic Avro Schema :
   * {
   *     "type":"record",
   *     "name":"student",
   *     "namespace":"org.apache",
   *     "doc":"",
   *     "fields":[
   *         {
   *             "name":"id1",
   *             "type":"int",
   *             "doc":""
   *         },
   *         {
   *             "name":"name1",
   *             "type":"string",
   *             "doc":""
   *         },
   *         {
   *             "name":"sex1",
   *             "type":{
   *                 "type":"enum",
   *                 "name":"SEX",
   *                 "doc":"催付结果",
   *                 "symbols":[
   *                     "男",
   *                     "女"]
   *             },
   *             "doc":"催付结果",
   *             "default":"男"
   *         },
   *         {
   *             "name":"env1",
   *             "type":{
   *                 "type":"enum",
   *                 "name":"Env",
   *                 "doc":"数据环境",
   *                 "symbols":[
   *                     "PRD",
   *                     "PRE"]
   *             },
   *             "doc":"数据环境",
   *             "default":"PRD"
   *         }]
   * }
   */
  test("auto fetch avro schema from kafka registry") {
    spark.sql(
      s"""
         |CREATE OR REPLACE TEMPORARY VIEW stu (id1 int, name1 string, sex1 string, env1 string)
         |USING org.apache.spark.sql.execution.datasources.KafkaSinkRelationProvider
         |OPTIONS (
         |  tag 'kafka',
         |  recordType 'avro',
         |  kafka.bootstrap.servers '$bootstrapServers',
         |  kafka.schema.registry.url '$schemaRegistryUrl',
         |  kafkaTopic 'test_wankun2',
         |  avro.forceCreate false
         |)
       """.stripMargin)

    spark.sql(
      s"""
         |INSERT INTO stu
         |SELECT 1 as id1, 'wankun' as name1,
         |       '男' as sex1, 'PRD' env1
         |""".stripMargin)

    // Check for result
    // kafka-avro-console-consumer  --property schema.registry.url=http://10.23.177.40:8081 --bootstrap-server 10.23.177.40:9092   --topic test_wankun2
    // {"id1":1,"name1":"wankun","sex1":"男","env1":"PRD"}
  }

  test("force create avro schema") {
    spark.sql(
      s"""
         |CREATE OR REPLACE TEMPORARY VIEW stu (name1 string, id1 int, sex1 string, env1 string, age1 int)
         |USING org.apache.spark.sql.execution.datasources.KafkaSinkRelationProvider
         |OPTIONS (
         |  tag 'kafka',
         |  recordType 'avro',
         |  kafka.bootstrap.servers '$bootstrapServers',
         |  kafka.schema.registry.url '$schemaRegistryUrl',
         |  kafkaTopic 'test_wankun2',
         |  avro.forceCreate true,
         |  avro.name 'student',
         |  avro.namespace 'org.apache'
         |)
       """.stripMargin)

    spark.sql(
      s"""
         |INSERT INTO stu
         |SELECT 'wankun' as name1, 2 as id1,
         |       '男' as sex1, 'PRD' env1, 33 as age1
         |""".stripMargin)

    // Check for result
    // kafka-avro-console-consumer  --property schema.registry.url=http://10.23.177.40:8081 --bootstrap-server 10.23.177.40:9092   --topic test_wankun2
    // {"id1":1,"name1":"wankun","sex1":"男","env1":"PRD"}
  }

  test("send avro record to kafka") {
    spark.sql(
      s"""
         |CREATE OR REPLACE TEMPORARY VIEW stu (id1 int, name1 string, sex1 string, env1 string)
         |USING org.apache.spark.sql.execution.datasources.KafkaSinkRelationProvider
         |OPTIONS (
         |  tag 'kafka',
         |  recordType 'avro',
         |  kafka.bootstrap.servers '$bootstrapServers',
         |  kafka.schema.registry.url '$schemaRegistryUrl',
         |  kafkaTopic 'test_wankun2',
         |  avro.name 'student',
         |  avro.namespace 'org.apache',
         |  avro.fieldMapping '
         |  [{
         |	"name": "sex1",
         |	"type": {
         |		"type": "enum",
         |		"name": "SEX",
         |		"doc": "催付结果",
         |		"symbols": ["男", "女"]
         |	},
         |	"doc": "催付结果",
         |	"default": "男"
         |}, {
         |	"name": "env1",
         |	"type": {
         |		"type": "enum",
         |		"name": "Env",
         |		"doc": "数据环境",
         |		"symbols": ["PRD", "PRE"]
         |	},
         |	"doc": "数据环境",
         |	"default": "PRD"
         |}]'
         |)
       """.stripMargin)

    spark.sql(
      s"""
         |INSERT INTO stu
         |SELECT 1 as id1, 'wankun' as name1,
         |       '男' as sex1, 'PRD' env1
         |""".stripMargin)

    // Check for result
    // kafka-avro-console-consumer  --property schema.registry.url=http://10.23.177.40:8081 --bootstrap-server 10.23.177.40:9092   --topic test_wankun2
    // {"id1":1,"name1":"wankun","sex1":"男","env1":"PRD"}
  }

  test("send avro record to kafka with format alias") {
    spark.sql(
      s"""
         |CREATE OR REPLACE TEMPORARY VIEW stu (id1 int, name1 string, sex1 string, env1 string)
         |USING kafka_sink
         |OPTIONS (
         |  tag 'kafka',
         |  recordType 'avro',
         |  kafka.bootstrap.servers '$bootstrapServers',
         |  kafka.schema.registry.url '$schemaRegistryUrl',
         |  kafkaTopic 'test_wankun2',
         |  avro.name 'student',
         |  avro.namespace 'org.apache',
         |  avro.fieldMapping '
         |  [{
         |	"name": "sex1",
         |	"type": {
         |		"type": "enum",
         |		"name": "SEX",
         |		"doc": "催付结果",
         |		"symbols": ["男", "女"]
         |	},
         |	"doc": "催付结果",
         |	"default": "男"
         |}, {
         |	"name": "env1",
         |	"type": {
         |		"type": "enum",
         |		"name": "Env",
         |		"doc": "数据环境",
         |		"symbols": ["PRD", "PRE"]
         |	},
         |	"doc": "数据环境",
         |	"default": "PRD"
         |}]'
         |)
       """.stripMargin)

  }

  test("test send avro record") {

    val properties = new Properties
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer])
    properties.put(ProducerConfig.ACKS_CONFIG, "all")
    properties.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl)
    val producer: KafkaProducer[String, Record] = new KafkaProducer[String, Record](properties)

    val avroSchema: Schema =
      new Schema.Parser().parse(
        s"""{
           |	"type": "record",
           |	"name": "PddMessageArrivedDetail",
           |	"namespace": "org.apachetech.chat.pdd",
           |	"doc": "",
           |	"fields": [{
           |		"name": "source",
           |		"type": {
           |			"type": "enum",
           |			"name": "REPORT_SOURCE",
           |			"doc": "拼多多异步返回调用结果",
           |			"symbols": ["HTTP", "PDD"]
           |		},
           |		"doc": "消息来源: HTTP, PDD",
           |		"default": "PDD"
           |	}, {
           |		"name": "arrived_time_in_ms",
           |		"type": "long",
           |		"doc": ""
           |	}, {
           |		"name": "message_type",
           |		"type": {
           |			"type": "enum",
           |			"name": "REPORT_MESSAGE_TYPE",
           |			"doc": "卡片",
           |			"symbols": ["MESSAGE", "CARD"]
           |		},
           |		"doc": "上报类型: CARD, MESSAGE",
           |		"default": "MESSAGE"
           |	}, {
           |		"name": "error_msg",
           |		"type": "string",
           |		"doc": ""
           |	}, {
           |		"name": "biz_id",
           |		"type": "string",
           |		"doc": ""
           |	}, {
           |		"name": "is_success",
           |		"type": "boolean",
           |		"doc": ""
           |	}, {
           |		"name": "error_code",
           |		"type": "int",
           |		"doc": ""
           |	}]
           |}
           |""".stripMargin)
    val record = new GenericData.Record(avroSchema)

    record.put("source", "PDD")
    record.put("arrived_time_in_ms", 1602950403691l)
    record.put("message_type", "MESSAGE")
    record.put("error_msg", "")
    record.put("biz_id", "45332260-6cf3-48a7-81d9-c6ca59879ae0")
    record.put("is_success", true)
    record.put("error_code", 0)

    producer.send(new ProducerRecord("test_topic", record))
  }
}