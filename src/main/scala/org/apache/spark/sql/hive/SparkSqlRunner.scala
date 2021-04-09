// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package org.apache.spark.sql.hive

import java.net.URL

import org.apache.hadoop.hive.cli.CliSessionState
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.catalyst.plans.logical.Repartition
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.execution.{QueryExecution, SQLExecution}
import org.apache.spark.sql.hive.client.HiveClientImpl
import org.apache.spark.sql.internal.SharedState
import org.apache.spark.sql.optimizer._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.udf.UDFFactory
import org.apache.spark.sql.util.{Logging, NextIterator}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.util.{MutableURLClassLoader, Utils}
import org.apache.sql.runner.callback.{DataCallBackFactory, QueryResult}
import org.apache.sql.runner.container.ConfigContainer

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2020-04-15.
 */
class SparkSqlRunner(implicit var spark: SparkSession) extends Logging {

  def run(sqlText: String): QueryResult = runWithErrorLog {
    spark.sparkContext.setJobDescription(sqlText)

    val queryExecution = {
      val logicalPlan = spark.sessionState.sqlParser.parsePlan(sqlText)
      val qe =
        if (ConfigContainer.getOrElse("rule.spark.sinksize", "0").toInt > 0) {
          new QueryExecution(spark, Repartition(1, shuffle = true, logicalPlan))
        } else {
          new QueryExecution(spark, logicalPlan)
        }
      qe.assertAnalyzed()
      qe
    }
    PartitionScanLimitRule(spark).apply(queryExecution.optimizedPlan)

    val (schema, iterator) = SQLExecution.withNewExecutionId(queryExecution) {
      val output = queryExecution.analyzed.output
      val schema: StructType = StructType.fromAttributes(output)
      val iterator: Iterator[InternalRow] = queryExecution.executedPlan.executeToIterator
      (schema, collectAsIterator(iterator, schema))
    }

    logDebug(s"queryExecution : ${queryExecution}")
    // collect metrics from executedPlan
    queryExecution.executedPlan.collectLeaves().map(SqlRunnerMetrics.logSparkPlanMetrics)
    SqlRunnerMetrics.logSparkPlanMetrics(queryExecution.executedPlan)

    QueryResult(schema, iterator)
  }

  def collectAsIterator(iterator: Iterator[InternalRow],
                        schema: StructType): NextIterator[GenericRowWithSchema] = {
    new NextIterator[GenericRowWithSchema] {

      val dataCallBacks =
        DataCallBackFactory.callBacks.get().filterNot(_.skipEmpty && iterator.isEmpty)

      dataCallBacks.foreach(_.init(schema))

      val converter = CatalystTypeConverters.createToScalaConverter(schema)

      override def getNext(): GenericRowWithSchema = {
        if (iterator.hasNext) {
          val row: Row = converter(iterator.next()).asInstanceOf[Row]
          new GenericRowWithSchema(row.toSeq.toArray, schema)
        } else {
          finished = true

          // iterator close
          dataCallBacks.foreach(_.close())
          DataCallBackFactory.clearDataCallBack()

          null.asInstanceOf[GenericRowWithSchema]
        }
      }

      override def close(): Unit = {}

      override def next(): GenericRowWithSchema = {
        val row = super.next()
        dataCallBacks.foreach(_.next(row))
        row
      }
    }
  }
}

object SparkSqlRunner extends Logging {

  val sparkClassLoader: MutableURLClassLoader = {
    val loader =
      new MutableURLClassLoader(new Array[URL](0),
        Thread.currentThread.getContextClassLoader)

    Thread.currentThread.setContextClassLoader(loader)
    loader
  }

  implicit def sparkSession(appNameOpt: Option[String] = None): SparkSession =
    SparkSession.synchronized {
      val sparkConf = new SparkConf(loadDefaults = true)
      Utils.loadDefaultSparkProperties(sparkConf)

      val hadoopConf = SparkHadoopUtil.get.newConfiguration(sparkConf)
      val extraConfigs = HiveUtils.formatTimeVarsForHiveClient(hadoopConf)

      val cliConf = HiveClientImpl.newHiveConf(sparkConf, hadoopConf, extraConfigs)

      val sessionState = new CliSessionState(cliConf)

      // Set all properties specified via command line.
      val conf: HiveConf = sessionState.getConf
      // Hive 2.0.0 onwards HiveConf.getClassLoader returns the UDFClassLoader (created by Hive).
      // Because of this spark cannot find the jars as class loader got changed
      // Hive changed the class loader because of HIVE-11878, so it is required to use old
      // classLoader as sparks loaded all the jars in this classLoader
      conf.setClassLoader(Thread.currentThread().getContextClassLoader)

      SharedState.loadHiveConfFile(sparkConf, conf)
      org.apache.hadoop.hive.ql.session.SessionState.start(sessionState)

      val appName = appNameOpt.getOrElse {
        sparkConf.getOption("spark.app.name")
          .getOrElse("SparkSQLRunner")
      }
      sparkConf.set("spark.yarn.queue", ConfigContainer.getOrElse("spark.yarn.queue", "default"))
      sparkConf.setAppName(appName)
      sparkConf.set("spark.sql.extensions", "org.apache.spark.sql.optimizer.InsightExtensions")
      sparkConf.set("spark.kryoserializer.buffer", "102400K")
      sparkConf.set("spark.executor.processTreeMetrics.enabled", "true")
      sparkConf.set("spark.yarn.am.memory", "1g")
      sparkConf.set("spark.executor.memory", "2g")
      sparkConf.set("spark.executor.memoryOverhead", "1g")

      // enable datasource v2 for kafka and jdbc
      sparkConf.set("spark.sql.catalog.jdbc", "org.apache.spark.sql.execution.datasources.jdbc.JDBCCatalog")
      sparkConf.set("spark.sql.catalog.kafka", "org.apache.spark.sql.execution.datasources.kafka.KafkaCatalog")

      ConfigContainer.valueMap.get
        .filter(config => config._1.startsWith("spark"))
        .foreach(config => sparkConf.set(config._1, config._2))

      implicit val spark = SparkSession.builder.config(sparkConf).enableHiveSupport()
        .getOrCreate()

      val parentSessionStateField = classOf[SparkSession].getDeclaredField("parentSessionState")
      parentSessionStateField.setAccessible(true)
      parentSessionStateField.set(spark, Some(new InsightSessionStateBuilder(spark).build()))

      UDFFactory.registerExternalUDFs(spark)

      // SPARK-29604: force initialization of the session state with the Spark class loader,
      // instead of having it happen during the initialization of the Hive client (which may use a
      // different class loader).
      spark.sessionState
      spark
    }
}
