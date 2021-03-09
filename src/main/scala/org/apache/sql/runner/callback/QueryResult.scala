// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package org.apache.sql.runner.callback

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.StructType

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2019-12-05.
 */
case class QueryResult(schema: StructType, iterator: Iterator[GenericRowWithSchema])
