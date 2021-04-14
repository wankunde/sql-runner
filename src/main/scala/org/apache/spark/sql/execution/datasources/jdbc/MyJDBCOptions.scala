// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package org.apache.spark.sql.execution.datasources.jdbc

import java.util.Locale

import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 *
 * @date 2021-04-08.
 *
 *      Spark内置的JDBCOptions 不会序列化用户传入的自定义属性，所以直接自己干
 */
case class MyJDBCOptions(@transient override val parameters: CaseInsensitiveMap[String])
  extends JDBCOptions(parameters) {

  import JDBCOptions._

  def this(parameters: Map[String, String]) = this(CaseInsensitiveMap(parameters))

  def this(url: String, table: String, parameters: Map[String, String]) = {
    this(CaseInsensitiveMap(parameters ++ Map(
      JDBCOptions.JDBC_URL -> url,
      JDBCOptions.JDBC_TABLE_NAME -> table)))
  }

  require(
    parameters.get(JDBC_TABLE_NAME).isDefined,
    s"Option '$JDBC_TABLE_NAME' is required. " +
      s"Option '$JDBC_QUERY_STRING' is not applicable while writing.")

  val uniqueKeys = parameters.getOrElse(MyJDBCOptions.JDBC_UNIQUE_KEYS, "")

  var filterWhereClause = parameters.getOrElse(MyJDBCOptions.JDBC_FILTER_WHERE_CLAUSE, "")

}

object MyJDBCOptions {

  private val jdbcOptionNames = collection.mutable.Set[String]()

  private def newOption(name: String): String = {
    jdbcOptionNames += name.toLowerCase(Locale.ROOT)
    name
  }

  val JDBC_URL = newOption("url")
  val JDBC_TABLE_NAME = newOption("dbtable")
  val JDBC_QUERY_STRING = newOption("query")
  val JDBC_DRIVER_CLASS = newOption("driver")
  val JDBC_PARTITION_COLUMN = newOption("partitionColumn")
  val JDBC_LOWER_BOUND = newOption("lowerBound")
  val JDBC_UPPER_BOUND = newOption("upperBound")
  val JDBC_NUM_PARTITIONS = newOption("numPartitions")
  val JDBC_QUERY_TIMEOUT = newOption("queryTimeout")
  val JDBC_BATCH_FETCH_SIZE = newOption("fetchsize")
  val JDBC_TRUNCATE = newOption("truncate")
  val JDBC_CASCADE_TRUNCATE = newOption("cascadeTruncate")
  val JDBC_CREATE_TABLE_OPTIONS = newOption("createTableOptions")
  val JDBC_CREATE_TABLE_COLUMN_TYPES = newOption("createTableColumnTypes")
  val JDBC_CUSTOM_DATAFRAME_COLUMN_TYPES = newOption("customSchema")
  val JDBC_BATCH_INSERT_SIZE = newOption("batchsize")
  val JDBC_TXN_ISOLATION_LEVEL = newOption("isolationLevel")
  val JDBC_SESSION_INIT_STATEMENT = newOption("sessionInitStatement")
  val JDBC_PUSHDOWN_PREDICATE = newOption("pushDownPredicate")
  val JDBC_UNIQUE_KEYS = newOption("uniqueKeys")
  val JDBC_FILTER_WHERE_CLAUSE = newOption("filterWhereClause")

}
