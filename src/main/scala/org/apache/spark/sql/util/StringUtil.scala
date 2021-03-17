// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package org.apache.spark.sql.util

import org.apache.commons.lang3.StringUtils

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2021-03-17.
 */
object StringUtil {

  val escapeMapping: Map[Array[Char], Array[Char]] = Map(
    Array('\"') -> Array('\"'),
    Array(''') -> Array('''),
    Array('(') -> Array(')'),
  )

  def escapeStringValue(text: String): String = {
    var res = text.trim
    for ((startChars, endChars) <- escapeMapping
         if res.startsWith(new String(startChars)) && res.endsWith(new String(endChars))) {
      res = StringUtils.removeStart(res, new String(startChars))
      res = StringUtils.removeEnd(res, new String(endChars)).trim
    }
    res
  }
}
