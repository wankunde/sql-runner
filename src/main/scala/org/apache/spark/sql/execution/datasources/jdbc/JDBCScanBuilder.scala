// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package org.apache.spark.sql.execution.datasources.jdbc

import java.sql.{Connection, PreparedStatement, ResultSet}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2021-04-07.
 */
class JDBCScanBuilder(schema: StructType, options: MyJDBCOptions) extends ScanBuilder {
  override def build(): Scan = new JDBCScan(schema, options)
}

class JDBCScan(schema: StructType, options: MyJDBCOptions) extends Scan {

  override def description(): String = options.tableOrQuery

  override def readSchema(): StructType = schema

  override def toBatch: Batch = new JDBCBatch(schema, options)
}

case class JDBCInputPartition(partitionId: Int) extends InputPartition

class JDBCBatch(schema: StructType, options: MyJDBCOptions) extends Batch {

  override def planInputPartitions(): Array[InputPartition] = {
    Range(0, options.numPartitions.get)
      .map(partitionId => JDBCInputPartition(partitionId)).toArray
  }

  override def createReaderFactory(): PartitionReaderFactory =
    new JDBCPartitionReaderFactory(schema, options)
}

class JDBCPartitionReaderFactory(schema: StructType, options: MyJDBCOptions)
  extends PartitionReaderFactory {
  override def createReader(inputPartition: InputPartition): PartitionReader[InternalRow] =
    new JDBCPartitionReader(schema, inputPartition.asInstanceOf[JDBCInputPartition], options)
}

class JDBCPartitionReader(schema: StructType,
                          inputPartition: JDBCInputPartition,
                          options: MyJDBCOptions)
  extends PartitionReader[InternalRow] with Logging {

  var (conn, stmt, rs, iterator) = initialize()

  override def next(): Boolean = iterator.hasNext

  override def get(): InternalRow = iterator.next()

  def initialize(): (Connection, PreparedStatement, ResultSet, Iterator[InternalRow]) = {
    val partitionId = inputPartition.partitionId
    import options._

    require(partitionColumn.nonEmpty, "partitionColumn should be set when reading jdbc table!")
    require(numPartitions.nonEmpty, "numPartitions should be set when reading jdbc table!")

    val conn: Connection = MyJDBCUtils.createConnectionFactory(options)()

    val dialect = JdbcDialects.get(url)
    dialect.beforeFetch(conn, options.asProperties.asScala.toMap)

    // This executes a generic SQL statement (or PL/SQL block) before reading
    // the table/query via JDBC. Use this feature to initialize the database
    // session environment, e.g. for optimizations and/or troubleshooting.
    options.sessionInitStatement match {
      case Some(sql) =>
        val statement = conn.prepareStatement(sql)
        logInfo(s"Executing sessionInitStatement: $sql")
        try {
          statement.setQueryTimeout(options.queryTimeout)
          statement.execute()
        } finally {
          statement.close()
        }
      case None =>
    }

    // H2's JDBC driver does not support the setSchema() method.  We pass a
    // fully-qualified table name in the SELECT statement.  I don't know how to
    // talk about a table in a completely portable way.
    val myWhereClause =
    if (partitionId == 0) {
      s"WHERE crc32(${partitionColumn.get}) % ${numPartitions.get} = $partitionId " +
        s"or ${partitionColumn.get} is null"
    } else {
      s"WHERE crc32(${partitionColumn.get}) % ${numPartitions.get} = $partitionId"
    }

    val sqlText = s"SELECT * FROM ${options.tableOrQuery} $myWhereClause"
    val stmt: PreparedStatement =
      conn.prepareStatement(sqlText,
        ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
    stmt.setFetchSize(options.fetchSize)
    stmt.setQueryTimeout(options.queryTimeout)

    val rs: ResultSet = stmt.executeQuery()
    val rowsIterator = MyJDBCUtils.resultSetToSparkInternalRows(rs, schema)
    (conn, stmt, rs, rowsIterator)
  }

  var closed = false

  override def close(): Unit = {
    if (closed) return
    try {
      if (null != rs) {
        rs.close()
      }
    } catch {
      case e: Exception => logWarning("Exception closing resultset", e)
    }
    try {
      if (null != stmt) {
        stmt.close()
      }
    } catch {
      case e: Exception => logWarning("Exception closing statement", e)
    }
    try {
      if (null != conn) {
        if (!conn.isClosed && !conn.getAutoCommit) {
          try {
            conn.commit()
          } catch {
            case NonFatal(e) => logWarning("Exception committing transaction", e)
          }
        }
        conn.close()
      }
      logInfo("closed connection")
    } catch {
      case e: Exception => logWarning("Exception closing connection", e)
    }
    closed = true
  }
}