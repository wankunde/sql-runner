// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package org.apache.spark.sql.execution.datasources

import java.sql.{Connection, PreparedStatement, Types}

import org.apache.spark.Partition
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils.createConnectionFactory
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JDBCRDD, JdbcOptionsInWrite, JdbcUtils}
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.sources.{BaseRelation, Filter, InsertableRelation}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.sql.runner.metrics.ReporterTrait

import scala.collection.mutable

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2020-06-01.
 */
case class JdbcUpsertRelation(override val schema: StructType,
                              parts: Array[Partition],
                              jdbcOptions: JDBCOptions)(@transient val sparkSession: SparkSession)
  extends BaseRelation
    with InsertableRelation
    with ReporterTrait
    with Logging {

  override def sqlContext: SQLContext = sparkSession.sqlContext

  override val needConversion: Boolean = false

  // Check if JDBCRDD.compileFilter can accept input filters
  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = {
    if (jdbcOptions.pushDownPredicate) {
      filters.filter(JDBCRDD.compileFilter(_, JdbcDialects.get(jdbcOptions.url)).isEmpty)
    } else {
      filters
    }
  }

  // A `JDBCValueSetter` is responsible for setting a value from `Row` into a field for
  // `PreparedStatement`. The last argument `Int` means the index for the value to be set
  // in the SQL statement and also used for the value in `Row`.
  private type JDBCValueSetter = (PreparedStatement, Row) => Unit

  private def makeSetter(fieldIndex: Int, pos: Int, dataType: DataType): JDBCValueSetter = dataType match {
    case IntegerType =>
      (stmt: PreparedStatement, row: Row) =>
        if (row.isNullAt(fieldIndex)) {
          stmt.setNull(pos, Types.INTEGER)
        } else {
          stmt.setInt(pos, row.getInt(fieldIndex))
        }
    case LongType =>
      (stmt: PreparedStatement, row: Row) =>
        if (row.isNullAt(fieldIndex)) {
          stmt.setNull(pos, Types.BIGINT)
        } else {
          stmt.setLong(pos, row.getLong(fieldIndex))
        }

    case DoubleType =>
      (stmt: PreparedStatement, row: Row) =>
        if (row.isNullAt(fieldIndex)) {
          stmt.setNull(pos, Types.DOUBLE)
        } else {
          stmt.setDouble(pos, row.getDouble(fieldIndex))
        }

    case FloatType =>
      (stmt: PreparedStatement, row: Row) =>
        if (row.isNullAt(fieldIndex)) {
          stmt.setNull(pos, Types.FLOAT)
        } else {
          stmt.setFloat(pos, row.getFloat(fieldIndex))
        }

    case ShortType =>
      (stmt: PreparedStatement, row: Row) =>
        if (row.isNullAt(fieldIndex)) {
          stmt.setNull(pos, Types.SMALLINT)
        } else {
          stmt.setInt(pos, row.getShort(fieldIndex))
        }

    case ByteType =>
      (stmt: PreparedStatement, row: Row) =>
        if (row.isNullAt(fieldIndex)) {
          stmt.setNull(pos, Types.TINYINT)
        } else {
          stmt.setInt(pos, row.getByte(fieldIndex))
        }

    case BooleanType =>
      (stmt: PreparedStatement, row: Row) =>
        if (row.isNullAt(fieldIndex)) {
          stmt.setNull(pos, Types.BOOLEAN)
        } else {
          stmt.setBoolean(pos, row.getBoolean(fieldIndex))
        }

    case StringType =>
      (stmt: PreparedStatement, row: Row) =>
        if (row.isNullAt(fieldIndex)) {
          stmt.setNull(pos, Types.VARCHAR)
        } else {
          stmt.setString(pos, row.getString(fieldIndex))
        }

    case BinaryType =>
      (stmt: PreparedStatement, row: Row) =>
        if (row.isNullAt(fieldIndex)) {
          stmt.setNull(pos, Types.BINARY)
        } else {
          stmt.setBytes(pos, row.getAs[Array[Byte]](fieldIndex))
        }

    case TimestampType =>
      (stmt: PreparedStatement, row: Row) =>
        if (row.isNullAt(fieldIndex)) {
          stmt.setNull(pos, Types.TIMESTAMP)
        } else {
          stmt.setTimestamp(pos, row.getAs[java.sql.Timestamp](fieldIndex))
        }

    case DateType =>
      (stmt: PreparedStatement, row: Row) =>
        if (row.isNullAt(fieldIndex)) {
          stmt.setNull(pos, Types.DATE)
        } else {
          stmt.setDate(pos, row.getAs[java.sql.Date](fieldIndex))
        }

    case t: DecimalType =>
      (stmt: PreparedStatement, row: Row) =>
        if (row.isNullAt(fieldIndex)) {
          stmt.setNull(pos, Types.DECIMAL)
        } else {
          stmt.setBigDecimal(pos, row.getDecimal(fieldIndex))
        }

    case _ =>
      (_: PreparedStatement, _: Row) =>
        throw new IllegalArgumentException(
          s"Can't translate non-null value for field $pos")
  }

  def pareStatement(rddSchema: StructType,
                    getConnection: () => Connection,
                    options: JdbcOptionsInWrite): (String, Array[String], Array[String]) = {
    val table = options.table
    val conn = getConnection()

    val pks: mutable.LinkedHashSet[String] =
      if (options.parameters.contains("unique.keys") && options.parameters("unique.keys").length > 0) {
        mutable.LinkedHashSet[String](options.parameters("unique.keys").split(",").map(_.trim.toLowerCase): _*)
      } else {
        throw new IllegalArgumentException("config unique.keys is needed.")
      }

    val tableSchema = JdbcUtils.getSchemaOption(conn, options)
    assert(tableSchema.isDefined, s"Fail to get $table in db, maybe $table does not exist")
    val tableColumnNames = tableSchema.get.fieldNames
    val rddSchemaNames = rddSchema.names.map(_.toLowerCase)
    val affectColumns = tableColumnNames.filter(col => rddSchemaNames.contains(col.toLowerCase))
    val updateColumns = affectColumns.filter(col => !pks.contains(col.toLowerCase))
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

  override def insert(df: DataFrame, overwrite: Boolean): Unit = {
    val rddSchema = df.schema

    val options = new JdbcOptionsInWrite(jdbcOptions.parameters)
    val getConnection: () => Connection = createConnectionFactory(options)
    val batchSize = options.batchSize

    val (upsertSql, affectColumns, updateColumns) =
      pareStatement(df.schema, getConnection, options)

    val repartitionedDF = options.numPartitions match {
      case Some(n) if n <= 0 => throw new IllegalArgumentException(
        s"Invalid value `$n` for parameter `${JDBCOptions.JDBC_NUM_PARTITIONS}` in table writing " +
          "via JDBC. The minimum value is 1.")
      case Some(n) if n < df.rdd.getNumPartitions => df.coalesce(n)
      case Some(n) if n > df.rdd.getNumPartitions => df.repartition(n)
      case _ => df
    }
    val upsertCount = sparkSession.sparkContext.longAccumulator(s"jdbc_upsert_${options.table}")
    repartitionedDF.rdd.foreachPartition { iterator =>
      val conn = getConnection()
      val stmt = conn.prepareStatement(upsertSql)
      val nameToIndex = rddSchema.names.map(_.toLowerCase).zipWithIndex.toMap
      val setters =
        (affectColumns ++ updateColumns).zipWithIndex.map { case (column, pos) =>
          val fieldIndex = nameToIndex(column.toLowerCase)
          makeSetter(fieldIndex, pos + 1, rddSchema.fields(fieldIndex).dataType)
        }

      try {
        var rowCount = 0
        stmt.setQueryTimeout(options.queryTimeout)
        while (iterator.hasNext) {
          val row = iterator.next()

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
            upsertCount.add(updateCounts)
            logInfo(s"commit JDBC PreparedStatement,affected rows = ${updateCounts}, " +
              s"statement counter = ${rowCount}")

            rowCount = 0
          }
        }

        val updateCounts = stmt.executeBatch().length
        upsertCount.add(updateCounts)
        logInfo(s"commit JDBC PreparedStatement,affected rows = ${updateCounts}, " +
          s"statement counter = ${rowCount}")
      } finally {
        // The stage must succeed.  We cannot propagate any exception close() might throw.
        try {
          conn.close()
        } catch {
          case e: Exception => logWarning("Transaction succeeded, but closing failed", e)
        }
      }
    }

    reportMetrics(s"stats.counters.insight.sink.mysql_upsert.${options.table}.count", upsertCount.value)
    logInfo(s"Jdbc Upsert rows counter ${upsertCount.value}")
    upsertCount.reset()
  }

  override def toString: String = {
    val partitioningInfo = if (parts.nonEmpty) s" [numPartitions=${parts.length}]" else ""
    // credentials should not be included in the plan output, table information is sufficient.
    s"JDBCRelation(${jdbcOptions.tableOrQuery})" + partitioningInfo
  }
}
