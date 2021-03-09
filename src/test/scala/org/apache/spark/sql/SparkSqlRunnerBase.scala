// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package org.apache.spark.sql

import org.apache.spark.sql.udf.UDFFactory
import org.apache.spark.internal.config.Tests.IS_TESTING
import org.apache.spark.sql.InsightSuiteUtils._
import org.apache.spark.sql.hive.SparkSqlRunner
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.test.SQLTestUtils

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2020-04-15.
 */
class SparkSqlRunnerBase extends QueryTest with SQLTestUtils with TestHiveSingleton {

  implicit val sparkImp: SparkSession = spark
  val sc = spark.sparkContext
  var runner: SparkSqlRunner = _

  override def beforeAll(): Unit = {

    super.beforeAll()
    System.setProperty(IS_TESTING.key, "true")
    cleanTestHiveData()

    SparkSession.active.sharedState.externalCatalog.addListener(InsightCatalogEventListener())
    UDFFactory.registerExternalUDFs(spark)

    runner = new SparkSqlRunner
  }


  override def afterAll() {
    cleanTestHiveData()
    SqlCommand.stop()
    super.afterAll()
  }
}
