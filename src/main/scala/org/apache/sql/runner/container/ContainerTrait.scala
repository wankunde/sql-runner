// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package org.apache.sql.runner.container

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2021-03-08.
 */
class ContainerTrait[A, B] {

  /**
   * 这里设计为 ThreadLocal 变量，用于支持多线程运行多job时，维护各自的配置信息.
   * 其他线程如果要维护自己的配置信息，从valueMap拷贝出去进行自己维护
   */
  val valueMap =
    new InheritableThreadLocal[Map[A, B]]() {
      override def initialValue(): Map[A, B] = Map[A, B]()
    }

  /**
   * 原有map和新的map合并，如果key冲突，保留新的map值
   *
   * @param map
   */
  def ++(map: Map[A, B]): Unit = {
    valueMap.set(valueMap.get() ++ map)
  }

  /**
   * 向map中加入新值，如果key已经存在，使用新值覆盖
   * @param kv
   */
  def +(kv: (A, B)): Unit = {
    valueMap.set(valueMap.get() + kv)
  }

  def getOrElse(key: A, default: => B): B = valueMap.get().getOrElse(key, default)

  def get(key: A): B = valueMap.get()(key)

  def getOption(key: A): Option[B] = valueMap.get().get(key)

  def contains(key: A): Boolean = valueMap.get().contains(key)

  def -(key: A): Unit = {
    if (valueMap.get().contains(key)) {
      valueMap.set(valueMap.get() - key)
    }
  }
}
