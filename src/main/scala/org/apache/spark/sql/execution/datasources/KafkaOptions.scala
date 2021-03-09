// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package org.apache.spark.sql.execution.datasources

import java.util.Properties

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import io.confluent.kafka.serializers.{AbstractKafkaAvroSerDeConfig, KafkaAvroSerializer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.apache.kafka.common.serialization.StringSerializer

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2020-07-13.
 */
case class KafkaOptions(config: Map[String, String]) {
  val tag: String = config.getOrElse("tag", "kafka")
  val recordType: String = config("recordType")
  val bootstrapServers = config(s"${tag}.bootstrap.servers")
  val topic = config("kafkaTopic")
  val viewName = topic.replaceAll("-", "_")
  val schemaRegistryUrl = config.getOrElse(s"${tag}.schema.registry.url", "")

  val avroName = config.getOrElse("avro.name", "")
  val avroNamespace = config.getOrElse("avro.namespace", "")
  val fieldMapping = config.getOrElse("avro.fieldMapping", "")
  val avroForceCreate = config.getOrElse("avro.forceCreate", "false")

  val maxRatePerPartition = config.getOrElse("maxRatePerPartition", "10000000").toInt

  lazy val fieldMappingMap = {
    val objectMapper = new ObjectMapper
    if (fieldMapping != "") {
      objectMapper.readTree(fieldMapping)
        .asScala
        .map(f => f.path("name").textValue() -> f)
        .toMap
    } else {
      Map[String, JsonNode]()
    }
  }

  lazy val serialClass: Class[_] = recordType match {
    case JSON_TYPE =>
      classOf[StringSerializer]
    case AVRO_TYPE =>
      classOf[KafkaAvroSerializer]
  }

  val JSON_TYPE: String = "json"
  val AVRO_TYPE: String = "avro"

  def initProducer[T: ClassTag](): KafkaProducer[String, T] = {
    val properties = new Properties
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, serialClass)
    properties.put(ProducerConfig.ACKS_CONFIG, "all")
    properties.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl)
    new KafkaProducer[String, T](properties)
  }
}
