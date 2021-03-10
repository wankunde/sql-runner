// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package org.apache.sql.runner.command

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.util.Logging

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2021-02-23.
 */
abstract class BaseCommand(sourceChars: SourceChars) extends Logging {

  val escapeMapping: Map[Array[Char], Array[Char]] = Map(
    Array('\"') -> Array('\"'),
    Array(''') -> Array('''),
    Array(''', ''', ''') -> Array(''', ''', '''),
    Array('(') -> Array(')'),
  )

  val chars = sourceChars.chars

  def readTo(char: Char): (String, Int, Int) = readTo(Array(char))

  def readTo(target: String): (String, Int, Int) = readTo(target.toCharArray)

  private def readTo(target: Array[Char]): (String, Int, Int) = {
    val len = target.length
    var index = -1
    var i = sourceChars.start
    while (i < sourceChars.end && index < 0) {
      // deal with escape char array
      for ((startChars, endChars) <- escapeMapping if startChars.intersect(target).size == 0) {
        val slen = startChars.length
        if (i > slen && chars(i - slen) != '\\') {
          if (chars.slice(i - slen + 1, i + 1) sameElements startChars) {
            val elen = endChars.length
            i = i + elen
            while (i < sourceChars.end && (chars(i - elen) == '\\' ||
              !(chars.slice(i - elen + 1, i + 1) sameElements endChars))) {
              i = i + 1
            }
          }
        }
      }

      if (chars.slice(i - len + 1, i + 1) sameElements target) {
        index = i + 1 - len
      } else {
        i = i + 1
      }
    }
    assert(index >= 0, s"Parse Job Error!\n${new String(chars.slice(sourceChars.start, sourceChars.end))}")
    var res = new String(chars.slice(sourceChars.start, index)).trim
    for ((startChars, endChars) <- escapeMapping
         if res.startsWith(new String(startChars)) && res.endsWith(new String(endChars))) {
      res = StringUtils.removeStart(res, new String(startChars))
      res = StringUtils.removeEnd(res, new String(endChars)).trim
    }
    val nextStart = i + 1
    (res, index, nextStart)
  }

  def run(): Unit = {
    throw new Exception("Unsupport Command!")
  }

  def dryrun(): Unit = run()
}
