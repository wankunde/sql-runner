// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package org.apache.spark.sql.execution.datasources.jdbc

import java.sql.Connection

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils.createConnectionFactory
import org.apache.spark.sql.execution.datasources.jdbc.MyJDBCUtils._
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.Logging

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2021-04-07.
 */
class JDBCDataWriter(schema: StructType, options: MyJDBCOptions)
  extends DataWriter[InternalRow] with Logging {

  val table = options.tableOrQuery
  val uniqueKeys: Set[String] =
    options.uniqueKeys.split(",").map(_.trim.toLowerCase).toSet

  val conn: Connection = createConnectionFactory(options)()
  conn.setAutoCommit(false)
  val (upsertSql, affectColumns, updateColumns) = upsertSqlAndColumns(conn, options)
  val stmt = conn.prepareStatement(upsertSql)

  val nameToIndex = schema.names.map(_.toLowerCase).zipWithIndex.toMap
  val setters =
    (affectColumns ++ updateColumns).zipWithIndex.map { case (column, pos) =>
      val fieldIndex = nameToIndex(column.toLowerCase)
      makeSetter(fieldIndex, pos + 1, schema.fields(fieldIndex).dataType)
    }

  var rowCount = 0
  val batchSize = options.batchSize

  override def write(row: InternalRow): Unit = {
    try {
      setters.map(_.apply(stmt, row))
    } catch {
      case e: Exception =>
        logError(s"fail to fill prepare statement params. Row=($row), statement=$stmt")
        throw e
    }

    stmt.addBatch()
    rowCount += 1
    if (rowCount % batchSize == 0) {
      val updateCounts = stmt.executeBatch().length
      //      upsertCount.add(updateCounts)
      logInfo(s"commit JDBC PreparedStatement,affected rows = ${updateCounts}, " +
        s"statement counter = ${rowCount}")

      rowCount = 0
    }
  }

  override def commit(): WriterCommitMessage = {
    val updateCounts = stmt.executeBatch().length
    //      upsertCount.add(updateCounts)
    logInfo(s"commit JDBC PreparedStatement,affected rows = ${updateCounts}, " +
      s"statement counter = ${rowCount}")
    conn.commit()
    new WriterCommitMessage() {}
  }

  override def abort(): Unit = {
    conn.rollback()
  }

  override def close(): Unit = {
    stmt.close()
    conn.close()
  }

  def upsertSqlAndColumns(conn: Connection,
                          options: JDBCOptions): (String, Array[String], Array[String]) = {
    val tableSchema = JdbcUtils.getSchemaOption(conn, options)
    assert(tableSchema.isDefined, s"Fail to get $table in db, maybe $table does not exist")
    val tableColumnNames = tableSchema.get.fieldNames
    val rddSchemaNames = schema.names.map(_.toLowerCase)
    val affectColumns = tableColumnNames.filter(col => rddSchemaNames.contains(col.toLowerCase))
    val updateColumns = affectColumns.filter(col => !uniqueKeys.contains(col.toLowerCase))
    tableColumnNames.filterNot(affectColumns.contains)
      .foreach(col => logWarning(s"row schema doesn't contains column : {${col} }"))

    val upsertSql =
      s"""
         |INSERT INTO ${table} (${affectColumns.mkString(", ")})
         |VALUES ( ${affectColumns.map(_ => "?").mkString(", ")} )
         |ON DUPLICATE KEY UPDATE ${updateColumns.map(_ + "= ?").mkString(", ")}
         |""".stripMargin
    logInfo(s"upsert sql : $upsertSql")
    (upsertSql, affectColumns, updateColumns)
  }
}
