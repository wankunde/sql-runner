// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package org.apache.sql.runner.callback

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.Logging

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2021-02-25.
 */
case class ExternalRelation(kind: String,
                            tag: String,
                            relationName: String,
                            ddl: String,
                            hasInitial: Boolean = false)
  extends DataCallBack with Logging {

  skipEmpty = false

  if (!hasInitial) {
    SparkSession.active.sql(ddl)
  }

  override def init(schema: StructType): Unit = {}

  override def next(row: GenericRowWithSchema): Unit = {}

  override def close(): Unit = {
    val dropRelationSql = s"DROP VIEW IF EXISTS `${relationName}`"
    logInfo(s"DROP external relation, \n$dropRelationSql")
    SparkSession.active.sql(dropRelationSql)
  }
}
