// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package org.apache.spark.sql.execution.datasources.kafka

import java.util

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{Identifier, SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.write.{BatchWrite, DataWriter, DataWriterFactory, LogicalWriteInfo, PhysicalWriteInfo, WriteBuilder, WriterCommitMessage}
import org.apache.spark.sql.execution.datasources.KafkaOptions
import org.apache.spark.sql.types.StructType
import org.apache.sql.runner.container.ConfigContainer

import scala.collection.JavaConverters._

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2021-04-06.
 */
case class KafkaTable(ident: Identifier) extends Table with SupportsWrite {

  override def name(): String = ident.toString

  override def schema(): StructType =
    StructType.fromDDL(ConfigContainer.get(s"${ident.toString}.schemaDDL"))

  override def capabilities(): util.Set[TableCapability] =
    Set(TableCapability.BATCH_WRITE).asJava

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder =
    new KafkaWriteBuilder(ident.name(), schema())
}

class KafkaWriteBuilder(name: String, schema: StructType) extends WriteBuilder {

  override def buildForBatch(): BatchWrite = new KafkaBatchWrite(name, schema)

}

class KafkaBatchWrite(name: String, schema: StructType) extends BatchWrite {

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory =
    new KafkaDataWriterFactory(name, schema)

  override def commit(messages: Array[WriterCommitMessage]): Unit = {}

  override def abort(messages: Array[WriterCommitMessage]): Unit = {}
}

class KafkaDataWriterFactory(name: String, schema: StructType) extends DataWriterFactory {

  val kafkaOption: KafkaOptions = KafkaOptions(name, ConfigContainer.valueMap.get())

  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] =
    new KafkaDataWriter(kafkaOption, schema)
}