// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package org.apache.spark.sql.util

import scala.beans.BeanProperty

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2020-02-26.
 */
case class GenericAvroSchema(@BeanProperty name: String,
                             @BeanProperty namespace: String,
                             @BeanProperty fields: Array[AvroField],
                             @BeanProperty `type`: String = "record",
                             @BeanProperty doc: String = "")

case class AvroField(@BeanProperty name: String,
                     @BeanProperty `type`: String,
                     @BeanProperty doc: String = "")
