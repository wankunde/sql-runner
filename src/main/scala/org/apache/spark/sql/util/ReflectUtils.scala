// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package org.apache.spark.sql.util

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2021-02-22.
 */
object ReflectUtils {

  /**
   * 通过反射执行private方法
   * @param clazz
   * @param name private 方法名
   * @param instance 方法执行的实例，如果是静态方法，直接传入null
   * @param parameterTypes 方法参数类型列表，无参数时传入空Seq()
   * @param parameters 方法参数实例列表，无参数时传入空Seq()
   */
  def runMethod(clazz: Class[_],
                name: String,
                instance: Any,
                parameterTypes: Seq[Class[_]],
                parameters: Seq[Object]): Unit = {
    val method = clazz.getDeclaredMethod(name, parameterTypes: _*)
    method.setAccessible(true)
    method.invoke(instance, parameters: _*)
  }

  def setVariable(instance: Any,
                  fieldName: String,
                  value: Any): Unit = {
    val field = instance.getClass.getDeclaredField(fieldName)
    field.setAccessible(true)
    field.set(instance, value)
  }
}
