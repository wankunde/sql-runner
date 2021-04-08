// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package org.apache.spark.sql.execution.datasources.kafka

import com.fasterxml.jackson.databind.node.{ArrayNode, JsonNodeFactory, ObjectNode}
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.google.common.util.concurrent.RateLimiter
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.serializers.subject.TopicNameStrategy
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericData.{EnumSymbol, Record}
import org.apache.commons.lang3.StringUtils.isNotBlank
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.execution.datasources.KafkaOptions
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.{AvroField, GenericAvroSchema, Logging}

import scala.collection.JavaConverters._

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2021-04-06.
 */
class KafkaDataWriter(kafkaOption: KafkaOptions, schema: StructType)
  extends DataWriter[InternalRow] with Logging {

  import kafkaOption._

  val rateLimiter = RateLimiter.create(maxRatePerPartition)

  var writeRows: Long = 0

  val fromRow = RowEncoder(schema).resolveAndBind().createDeserializer()

  lazy val jsonProducer: KafkaProducer[String, String] = initProducer[String]()
  lazy val avroProducer: KafkaProducer[String, Record] = initProducer[Record]()

  lazy val avroSchema: Schema = {
    if (isNotBlank(avroForceCreate) && avroForceCreate.toBoolean) {
      parseAvroSchemaByDF()
    } else {
      try {
        val strategy = new TopicNameStrategy()
        val subject = strategy.subjectName(topic, false, null)
        val schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 10)
        val schemaMetadata = schemaRegistryClient.getLatestSchemaMetadata(subject)
        val schema = schemaMetadata.getSchema
        new Schema.Parser().parse(schema)
      } catch {
        case e: Exception =>
          logError("Failed to get avro schema from registry.", e)
          parseAvroSchemaByDF()
      }
    }
  }

  def parseAvroSchemaByDF(): Schema = {
    assert(isNotBlank(avroName), "Avro Name should not be null!")
    assert(isNotBlank(avroNamespace), "Avro NameSpace should not be null!")
    val avroFields = schema.fields.map {
      f =>
        val avroType = f.dataType match {
          case _: BooleanType => "boolean"
          case _: IntegerType => "int"
          case _: LongType => "long"
          case _: DoubleType => "double"
          case _: FloatType => "float"
          case _: StringType => "string"
          case _: DecimalType => "double"
          case _ =>
            throw new IllegalArgumentException(
              s"Unsupported type ${
                f.dataType
              }"
            )
        }
        AvroField(f.name, avroType)
    }
    val genericAvroSchema = GenericAvroSchema(avroName, avroNamespace, avroFields)
    val schemaTree: JsonNode = new ObjectMapper().valueToTree(genericAvroSchema)
    val oldFieldsMap: Map[String, JsonNode] = schemaTree.path("fields")
      .asScala
      .map(f => f.path("name").textValue() -> f)
      .toMap
    val newFieldsList = (oldFieldsMap ++ fieldMappingMap).values.toList.asJava
    val newFields: ArrayNode = JsonNodeFactory.instance.arrayNode().addAll(newFieldsList)
    schemaTree.asInstanceOf[ObjectNode].set("fields", newFields)

    // keep the fields sorted as the output DataFrame
    val mergedSchema = new Schema.Parser().parse(schemaTree.toString)
    val sortedFields =
      schema.fieldNames.map {
        fieldName =>
          val field = mergedSchema.getField(fieldName)
          new Schema.Field(field.name, field.schema, field.doc, field.defaultVal)
      }.toList.asJava
    Schema.createRecord(mergedSchema.getName, mergedSchema.getDoc, mergedSchema.getNamespace,
      mergedSchema.isError, sortedFields)
  }

  override def write(internalRow: InternalRow): Unit = {
    val row = fromRow(internalRow)

    recordType match {
      case JSON_TYPE =>
        jsonProducer.send(new ProducerRecord(topic, row.json))
      case AVRO_TYPE =>
        val record = new GenericData.Record(avroSchema)
        try {
          schema.fields.zipWithIndex.map {
            case (field, fieldIndex) =>
              val fieldValue = avroSchema.getField(field.name)
              assert(fieldValue != null, s"Field ${field.name} not find in schema $avroSchema")
              val avroFiledSchema = fieldValue.schema()
              if (avroFiledSchema.getType == Schema.Type.ENUM) {
                record.put(field.name,
                  new EnumSymbol(avroFiledSchema, row.get(fieldIndex).toString))
              } else {
                record.put(field.name, row.get(fieldIndex))
              }
          }
          avroProducer.send(new ProducerRecord(topic, record))
        } catch {
          case ex: Exception =>
            logError(s"发送record to kafka 失败! schema: $avroSchema record:$record", ex)
            throw ex
        }
    }

    writeRows = writeRows + 1
  }

  override def commit(): WriterCommitMessage = {
    recordType match {
      case JSON_TYPE =>
        jsonProducer.flush()
      case AVRO_TYPE =>
        avroProducer.flush()
    }
    new WriterCommitMessage() {}
  }

  override def abort(): Unit = {}

  override def close(): Unit = {
    recordType match {
      case JSON_TYPE =>
        jsonProducer.close()
      case AVRO_TYPE =>
        avroProducer.close()
    }
  }
}
