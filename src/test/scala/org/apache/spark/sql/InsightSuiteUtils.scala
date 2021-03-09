// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package org.apache.spark.sql

import java.io.File

import org.apache.commons.io.FileUtils

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2020-09-16.
 */
object InsightSuiteUtils {

  def cleanTestHiveData(): Unit = {
    val metastoreDB = new File("metastore_db")
    if (metastoreDB.exists) {
      FileUtils.forceDelete(metastoreDB)
    }
    val sparkWarehouse = new File("spark-warehouse")
    if (sparkWarehouse.exists) {
      FileUtils.forceDelete(sparkWarehouse)
    }
  }
}
