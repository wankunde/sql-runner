// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package org.apache.spark.sql.execution.datasources.jdbc

import java.sql.Connection
import java.util

import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.sql.runner.container.ConfigContainer

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2021-04-07.
 *
 * 一般的实现里，会有一个Source类，继承 RelationProvider 和 TableProvider，负责提供Relation 和 Table对象。
 * 然后调用 DataSourceV2Utils.getTableFromProvider() 方法，从Provider 获取table实例的方法，但是我感觉这样
 * 还不如直接new 一个Table实例方便，那样做反而更绕了～～
 */
case class JDBCTable(ident: Identifier) extends Table
  with SupportsRead
  with SupportsWrite {

  import MyJDBCOptions._

  val namespace = ident.namespace()(0)
  val relationName = ident.name()

  val tableOrQuery =
    ConfigContainer.getOrElse(s"$namespace.$relationName.query", ident.name())

  val jdbcOptions = {
    val parameters = mutable.Map(
      JDBC_URL -> ConfigContainer.get(s"$namespace.url"),
      "user" -> ConfigContainer.get(s"$namespace.username"),
      "password" -> ConfigContainer.get(s"$namespace.password"),
      JDBC_TABLE_NAME -> tableOrQuery
    )
    Seq(
      JDBC_PARTITION_COLUMN,
      JDBC_NUM_PARTITIONS,
      JDBC_QUERY_TIMEOUT,
      JDBC_BATCH_FETCH_SIZE,
      JDBC_PUSHDOWN_PREDICATE,
      JDBC_UNIQUE_KEYS
    ).map(optionName => optionName -> s"$namespace.$relationName.$optionName")
      .filter(option => ConfigContainer.contains(option._2))
      .foreach { option => parameters += (option._1 -> ConfigContainer.get(option._2)) }

    // 读数据使用新的分区算法，JDBC_PARTITION_COLUMN 为必须参数，JDBC_LOWER_BOUND, JDBC_UPPER_BOUND 传入伪参数
    if (parameters.contains(JDBC_PARTITION_COLUMN)) {
      parameters += (JDBC_LOWER_BOUND -> "0")
      parameters += (JDBC_UPPER_BOUND -> "0")
    }

    // JDBC 更新数据时需要准备好更新的表的数据主键
    new MyJDBCOptions(parameters.toMap)
  }

  override def name(): String = ident.toString

  /**
   * JDBC表写的时候，schema通过child Plan自动解析生成
   * JDBC表读的时候，进行schema自动推测
   * @return
   */
  override def schema(): StructType = {
    if (ConfigContainer.contains(s"${ident.toString}.schemaDDL")) {
      StructType.fromDDL(ConfigContainer.get(s"${ident.toString}.schemaDDL"))
    } else {
      val conn: Connection = MyJDBCUtils.createConnectionFactory(jdbcOptions)()
      try {
        JdbcUtils.getSchemaOption(conn, jdbcOptions).get
      } finally {
        conn.close()
      }
    }
  }

  override def capabilities(): util.Set[TableCapability] =
    Set(TableCapability.BATCH_READ,
      TableCapability.BATCH_WRITE).asJava

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    Seq(
      JDBC_URL,
      "user",
      "password",
      JDBC_TABLE_NAME,
      JDBC_PARTITION_COLUMN,
      JDBC_NUM_PARTITIONS
    ).foreach { option =>
      require(jdbcOptions.parameters.contains(option),
        s"parameter $option is needed in JDBC read")
    }

    new JDBCScanBuilder(schema, jdbcOptions)
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    Seq(
      JDBC_URL,
      "user",
      "password",
      JDBC_TABLE_NAME,
      JDBC_UNIQUE_KEYS
    ).foreach { option =>
      require(jdbcOptions.parameters.contains(option),
        s"parameter $option is needed in JDBC write")
    }

    new JDBCWriteBuilder(schema, jdbcOptions)
  }
}
