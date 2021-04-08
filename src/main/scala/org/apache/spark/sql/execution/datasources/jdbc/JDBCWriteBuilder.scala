// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package org.apache.spark.sql.execution.datasources.jdbc

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.types.StructType

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2021-04-07.
 */
class JDBCWriteBuilder(schema: StructType, options: MyJDBCOptions) extends WriteBuilder {

  override def buildForBatch(): BatchWrite = new JDBCBatchWrite(schema, options)

}

class JDBCBatchWrite(schema: StructType, options: MyJDBCOptions) extends BatchWrite {

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory =
    new JDBCDataWriterFactory(schema, options)

  override def commit(messages: Array[WriterCommitMessage]): Unit = {}

  override def abort(messages: Array[WriterCommitMessage]): Unit = {}
}

class JDBCDataWriterFactory(schema: StructType, options: MyJDBCOptions) extends DataWriterFactory {

  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] =
    new JDBCDataWriter(schema, options)
}
