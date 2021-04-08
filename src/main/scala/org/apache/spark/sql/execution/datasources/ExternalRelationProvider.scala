// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JDBCRelation}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2020-07-13.
 */
class JdbcUpsertRelationProvider
  extends RelationProvider
    with SchemaRelationProvider
    with DataSourceRegister {

  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]): BaseRelation = {
    val jdbcOptions = new JDBCOptions(parameters)
    val resolver = sqlContext.conf.resolver
    val schema = JDBCRelation.getSchema(resolver, jdbcOptions)
    createRelation(sqlContext, parameters, schema)
  }

  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String],
                              schema: StructType): BaseRelation = {
    val jdbcOptions = new JDBCOptions(parameters)
    val resolver = sqlContext.conf.resolver
    val timeZoneId = sqlContext.conf.sessionLocalTimeZone
    val parts = JDBCRelation.columnPartition(schema, resolver, timeZoneId, jdbcOptions)
    JdbcUpsertRelation(schema, parts, jdbcOptions)(sqlContext.sparkSession)
  }

  override def shortName(): String = "mysql_upsert_sink"
}

class KafkaSinkRelationProvider
  extends SchemaRelationProvider
    with DataSourceRegister {

  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String],
                              schema: StructType): BaseRelation = {
    KafkaSinkRelation(schema, KafkaOptions("", parameters))(sqlContext.sparkSession)
  }

  override def shortName(): String = "kafka_sink"
}
