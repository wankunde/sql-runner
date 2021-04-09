// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package org.apache.spark.sql.execution.datasources.kafka

import org.apache.spark.sql.connector.catalog.{DelegatingCatalogExtension, Identifier, Table}
import org.apache.spark.sql.util.Logging

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2021-04-08.
 */
class KafkaCatalog extends DelegatingCatalogExtension with Logging {

  override def name(): String = "KAFKA"

  override def loadTable(ident: Identifier): Table = KafkaTable(ident)
}
