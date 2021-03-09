// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package org.apache.sql.runner.config

import java.time.LocalDateTime

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2020-03-06.
 */
object Configuration extends ContainerTrait[String, String] {

  def withSubstitution(substitution: VariableSubstitution)
                      (body: VariableSubstitution => Unit): Unit = {
    val originConfigMap = valueMap.get()
    val newConfigMap = originConfigMap.map {
      case (k, v) => k -> substitution.substitute(v)
    }
    valueMap.set(newConfigMap)
    try {
      body(substitution)
    } finally {
      valueMap.set(originConfigMap)
    }
  }

  private val batchTimeValue: ThreadLocal[LocalDateTime] = new ThreadLocal[LocalDateTime]()

  def setBatchTime(value: LocalDateTime): Unit = {
    batchTimeValue.set(value)
  }

  def getBatchTime(): LocalDateTime = {
    batchTimeValue.get()
  }

}