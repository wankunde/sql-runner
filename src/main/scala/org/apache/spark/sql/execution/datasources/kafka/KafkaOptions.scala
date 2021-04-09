// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package org.apache.spark.sql.execution.datasources.kafka

import java.util.Properties

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import io.confluent.kafka.serializers.{AbstractKafkaAvroSerDeConfig, KafkaAvroSerializer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.apache.kafka.common.serialization.StringSerializer

import scala.reflect.ClassTag

import scala.collection.JavaConverters._

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2020-07-13.
 */
case class KafkaOptions(name: String, config: Map[String, String]) extends Serializable {
  val bootstrapServers = config(s"kafka.bootstrap.servers")
  val schemaRegistryUrl = config.getOrElse(s"kafka.schema.registry.url", "")

  val topic = config(s"kafka.${name}.kafkaTopic")
  val recordType: String = config(s"kafka.${name}.recordType")
  val avroName = config.getOrElse(s"kafka.${name}.avro.name", "")
  val avroNamespace = config.getOrElse(s"kafka.${name}.avro.namespace", "")
  val fieldMapping = config.getOrElse(s"kafka.${name}.avro.fieldMapping", "")
  val avroForceCreate = config.getOrElse(s"kafka.${name}.avro.forceCreate", "false")

  val maxRatePerPartition = config.getOrElse(s"kafka.${name}.maxRatePerPartition", "10000000").toInt

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
