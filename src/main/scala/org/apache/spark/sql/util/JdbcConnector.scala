// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package org.apache.spark.sql.util

import java.sql.{Connection, PreparedStatement, SQLException}

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}
import org.apache.spark.sql.types._

/**
 * 1. 提供JDBC相关配置参数
 * 2. 提供JDBCOption实例 作为connect参数
 * 3. 提供JDBC相关操作util方法
 *
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2019-12-11.
 */
class JdbcConnector(config: Map[String, String]) extends Logging {

  val tag: String = config.getOrElse(
    "tag",
    throw new IllegalArgumentException("config tag is needed.")
  )

  /**
   * 1. Get ${tag.key} value from config map
   * 2. Return default value if or defaultValue is not empty
   * 3. throw parameter should be provided exception
   *
   * @param key
   * @param defaultValue
   * @return
   */
  def getJdbcConfig(key: String, defaultValue: String = ""): String = {
    config.get(s"$tag.$key") match {
      case Some(v) => v
      case None if defaultValue != "" => defaultValue
      case None => throw new Exception(s"parameter $key should be provided!")
    }
  }

  val url = getJdbcConfig("url")
  val username = getJdbcConfig("username")
  val password = getJdbcConfig("password")
  val queryTimeout = getJdbcConfig("query.timeout", "180").toInt
  val tableName: String = config("tableName")

  val jdbcConnectOption: JDBCOptions =
    new JDBCOptions(Map(
      JDBCOptions.JDBC_URL -> url,
      "user" -> username,
      "password" -> password,
      JDBCOptions.JDBC_TABLE_NAME -> tableName,
      JDBCOptions.JDBC_QUERY_TIMEOUT -> queryTimeout.toString
    ))

  def getConnection(): Connection = JdbcUtils.createConnectionFactory(jdbcConnectOption)()

  def closeConnection(conn: Connection): Unit = {
    try {
      if (conn != null) {
        conn.close()
      }
    } catch {
      case ex: Exception => logError("close jdbc connection error!", ex)
    }
  }

  def withConnection[T](body: Connection => T): T = {
    val conn: Connection = getConnection()
    try {
      body(conn)
    } catch {
      case ex: Exception =>
        logError("execute jdbc function error!", ex)
        throw ex
    } finally {
      closeConnection(conn)
    }
  }

  def getTableSchema(): StructType = {
    val tableSchemaOption = JdbcUtils.getSchemaOption(getConnection(), jdbcConnectOption)
    assert(tableSchemaOption.isDefined, s"Failed to get $tableName schema!")
    tableSchemaOption.get
  }

  /**
   * @param row 准备转换的Row数据
   * @param pstmt JDBC PreparedStatement
   * @param fields 需要转换的字段列表, pstmt在进行参数转换时的开始下标，默认为1
   */
  def rowToPreparedStatement(row: GenericRowWithSchema,
                             pstmt: PreparedStatement,
                             fields: Seq[StructField]): Unit = {
    fields.zipWithIndex.map {
      case (field, fieldIndex) =>
        field.dataType match {
          case _: BooleanType =>
            pstmt.setBoolean(fieldIndex + 1, row.getAs(field.name))
          case _: DoubleType =>
            pstmt.setDouble(fieldIndex + 1, row.getAs(field.name))
          case _: DecimalType =>
            pstmt.setBigDecimal(fieldIndex + 1, row.getAs(field.name))
          case _: FloatType =>
            pstmt.setFloat(fieldIndex + 1, row.getAs(field.name))
          case _: ByteType =>
            pstmt.setByte(fieldIndex + 1, row.getAs(field.name))
          case _: ShortType =>
            pstmt.setShort(fieldIndex + 1, row.getAs(field.name))
          case _: IntegerType =>
            pstmt.setInt(fieldIndex + 1, row.getAs(field.name))
          case _: LongType =>
            pstmt.setLong(fieldIndex + 1, row.getAs(field.name))
          case _: StringType =>
            pstmt.setString(fieldIndex + 1, row.getAs(field.name))
          case _: DateType =>
            pstmt.setDate(fieldIndex + 1, row.getAs(field.name))
          case _ =>
            throw new IllegalArgumentException(
              s"Unsupported type ${field.dataType}"
            )
        }
    }
  }

  var statementCounter: Long = 0

  def tryStatement[T](pstmt: PreparedStatement, row: Option[GenericRowWithSchema] = None)
                     (body: PreparedStatement => Unit): Unit = {
    try {
      statementCounter.synchronized {
        if (pstmt != null) {
          body(pstmt)
          statementCounter = statementCounter + 1
        }
        if (statementCounter % 10000 == 0) {
          val updateCounts = pstmt.executeBatch
          logInfo(s"commit JDBC PreparedStatement,affected rows = ${updateCounts.length}, " +
            s"statement counter = ${statementCounter}")
          pstmt.clearParameters()
        }
      }
    } catch {
      case e: Exception =>
        logError(s"debug message for pstmt : ${pstmt}, row : ${row}")
        throw e
    }
  }
}
