// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package org.apache.spark.sql.util

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.types.StringType

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2021-03-08.
 */
object OptimizerUtil {

  def parseHintParameter(value: Any): String = {
    value match {
      case v: String => UnresolvedAttribute.parseAttributeName(v).mkString(".")
      case Literal(v, dt: StringType) => v.toString
      case v: UnresolvedAttribute => v.nameParts.mkString(".")
      case unsupported => throw new AnalysisException(s"Unable to parse : $unsupported")
    }
  }
}
