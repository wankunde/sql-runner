// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package org.apache.spark.sql.optimizer

import java.sql.Connection
import java.util.Locale

import org.apache.commons.lang3.StringUtils
import org.apache.spark.Partition
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoStatement, LogicalPlan, UnresolvedHint, With}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions._
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils.createConnectionFactory
import org.apache.spark.sql.execution.datasources.jdbc._
import org.apache.spark.sql.util.OptimizerUtil.parseHintParameter
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.sql.runner.callback.{DataCallBackFactory, DingTalkSink, EmailSink, ExternalRelation}
import org.apache.sql.runner.container.ConfigContainer

import scala.collection.mutable

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2020-09-15.
 */
case class ExternalRelationRule(spark: SparkSession) extends Rule[LogicalPlan] {

  def createJDBCSourceRelation(tag: String, relationName: String): Unit = {
    assert(ConfigContainer.contains(s"$tag.type") &&
      ConfigContainer.get(s"$tag.type") == "jdbc", s"Fail to find config: $tag.type")

    // query and dbtable can not be specified together. If query is specified, just use it.
    // query example: (select c1, c2 from t1) as subq
    val tableOrQuery = ConfigContainer.getOrElse(s"$tag.$relationName.query", relationName)
    val viewParams = mutable.Map(
      JDBC_URL -> ConfigContainer.get(s"$tag.url"),
      JDBC_TABLE_NAME -> tableOrQuery,
      "user" -> ConfigContainer.get(s"$tag.username"),
      "password" -> ConfigContainer.get(s"$tag.password")
    )

    Seq("partitionColumn", "numPartitions", "queryTimeout",
      "fetchsize", "pushDownPredicate").foreach { option =>
      if (ConfigContainer.contains(s"$tag.$relationName.$option")) {
        viewParams += option -> ConfigContainer.get(s"$tag.$relationName.$option")
      }
    }

    if (viewParams.contains("partitionColumn") && viewParams.contains("numPartitions")) {
      viewParams += "lowerBound" -> Integer.MIN_VALUE.toString
      viewParams += "upperBound" -> Integer.MAX_VALUE.toString
    }
    val options = new JdbcOptionsInWrite(CaseInsensitiveMap(viewParams.toMap))
    val getConnection: () => Connection = createConnectionFactory(options)

    val tableSchema = JdbcUtils.getSchemaOption(getConnection(), options)
    assert(tableSchema.isDefined, s"Fail to get $relationName schema in $tag")

    // Spark内置的查询分区算法太差劲了，自己写吧～～
    val parts: Array[Partition] =
      if (viewParams.contains("partitionColumn") && viewParams.contains("numPartitions")) {
        val partitionColumn = viewParams("partitionColumn")
        val numPartitions = viewParams("numPartitions").toInt
        val predicates = Range(0, numPartitions).map(part =>
          if (part == 0) {
            s" crc32($partitionColumn) % $numPartitions = $part or $partitionColumn is null"
          } else {
            s" crc32($partitionColumn) % $numPartitions = $part"
          }
        ).toArray
        predicates.zipWithIndex.map { case (part, i) =>
          JDBCPartition(part, i): Partition
        }
      } else {
        Array[Partition](JDBCPartition(null, 0))
      }
    val relation = JDBCRelation(parts, options)(spark)
    val relationDF = spark.baseRelationToDataFrame(relation)
    relationDF.createOrReplaceTempView(relationName)

    val optionsString =
      viewParams.filterKeys(key => key != "lowerBound" && key != "upperBound")
        .map {
          case (k, v) =>
            if (k == "password") {
              s"$k '*************'"
            } else {
              s"$k '${v.replaceAll("'", "\"")}'"
            }
        }.mkString(",")
    val relationDDL =
      s"""
         |CREATE OR REPLACE TEMPORARY VIEW ${relationName} (${tableSchema.get.toDDL})
         |USING jdbc
         |OPTIONS ($optionsString)
         |""".stripMargin
    DataCallBackFactory.registerDataCallBack(
      ExternalRelation("JDBC", tag, relationName, relationDDL, true)
    )
  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperatorsUp {
    case hint@UnresolvedHint(hintName, parameters, child) => hintName.toUpperCase(Locale.ROOT) match {
      case "JDBC_VIEW" =>
        val tag: String = parseHintParameter(parameters(0))
        val relationName: String = parameters(1) match {
          case relationName: String => UnresolvedAttribute.parseAttributeName(relationName).head
          case relationName: UnresolvedAttribute => relationName.nameParts.head
          case unsupported => throw new AnalysisException(s"Unable to parse : $unsupported")
        }
        createJDBCSourceRelation(tag, relationName)

        child

      case "JDBC_SINK" =>
        val tag: String = parseHintParameter(parameters(0))
        val relationName: String = parseHintParameter(parameters(1))
        val relationAlias: String =
          if (parameters.size >= 3) {
            parseHintParameter(parameters(2))
          } else {
            relationName
          }

        val ddl = outputDDL(plan)
        createJdbcSinkRelation(tag, relationName, relationAlias, ddl)

        child

      case "KAFKA_SINK" =>
        val relationName: String = parseHintParameter(parameters(0))
        val ddl = outputDDL(plan)

        val tag = ConfigContainer.getOrElse("tag", "kafka")
        val viewParams =
          Map(
            "tag" -> tag,
            "recordType" -> ConfigContainer.get(s"$tag.recordType"),
            "maxRatePerPartition" -> ConfigContainer.getOrElse(s"${tag}.maxRatePerPartition", "10000000"),
            "kafka.bootstrap.servers" -> ConfigContainer.get(s"${tag}.bootstrap.servers"),
            "kafka.schema.registry.url" -> ConfigContainer.getOrElse(s"${tag}.schema.registry.url", ""),
            "kafkaTopic" -> ConfigContainer.get(s"${tag}.${relationName}.kafkaTopic"),
            "avro.name" -> ConfigContainer.getOrElse(s"${tag}.${relationName}.avro.name", ""),
            "avro.namespace" -> ConfigContainer.getOrElse(s"${tag}.${relationName}.avro.namespace", ""),
            "avro.fieldMapping" -> ConfigContainer.getOrElse(s"${tag}.${relationName}.avro.fieldMapping", ""),
            "avro.forceCreate" -> ConfigContainer.getOrElse(s"${tag}.${relationName}.avro.forceCreate", "false"))
        val tableOption =
          viewParams.filter(tup => StringUtils.isNotBlank(tup._2))
            .map(tup => s"${tup._1} '${tup._2}'").mkString(",")

        val relationDDL =
          s"""
             |CREATE OR REPLACE TEMPORARY VIEW `${relationName}` (${ddl})
             |USING kafka_sink
             |OPTIONS (${tableOption})
             |""".stripMargin

        DataCallBackFactory.registerDataCallBack(ExternalRelation("KAFKA_SINK", tag, relationName, relationDDL))

        child

      case "EMAIL_SINK" =>
        val name = parseHintParameter(parameters(0))
        DataCallBackFactory.registerDataCallBack(EmailSink(name, ConfigContainer.valueMap.get()))
        child

      case "DINGDING_SINK" =>
        val botName = parseHintParameter(parameters(0))
        DataCallBackFactory.registerDataCallBack(DingTalkSink(botName, ConfigContainer.valueMap.get()))
        child

      case _ => hint
    }
  }

  def outputDDL(plan: LogicalPlan): String = {
    plan transformUp {
      // ignore Unknown Hint
      case UnresolvedHint(_, _, child) => child
      case p => p
    } match {
      case InsertIntoStatement(_, _, child: LogicalPlan, _, _) =>
        new QueryExecution(spark, child).analyzed.schema.toDDL

      case With(InsertIntoStatement(_, _, child: LogicalPlan, _, _), cteRelations) =>
        new QueryExecution(spark, With(child, cteRelations)).analyzed.schema.toDDL

      case _ => throw new AnalysisException(s"Unsupported plan $plan")
    }
  }

  def createJdbcSinkRelation(tag: String,
                             relationName: String,
                             relationAlias: String,
                             ddl: String): Unit = {
    val viewParams =
      mutable.Map(
        "url" -> ConfigContainer.get(s"$tag.url"),
        "dbtable" -> relationName,
        "queryTimeout" -> ConfigContainer.getOrElse(s"$tag.queryTimeout", "180"),
        "user" -> ConfigContainer.get(s"$tag.username"),
        "password" -> ConfigContainer.get(s"$tag.password"),
        "unique.keys" -> ConfigContainer.get(s"$tag.$relationAlias.unique.keys"))
    if (ConfigContainer.contains(s"$tag.$relationAlias.numPartitions")) {
      viewParams += "numPartitions" -> ConfigContainer.get(s"$tag.$relationAlias.numPartitions")
    }
    val tableOption =
      viewParams.filter(tup => StringUtils.isNotBlank(tup._2))
        .map(tup => s"${tup._1} '${tup._2}'").mkString(",")

    val relationDDL =
      s"""
         |CREATE OR REPLACE TEMPORARY VIEW ${relationAlias} (${ddl})
         |USING mysql_upsert_sink
         |OPTIONS (${tableOption})
         |""".stripMargin
    DataCallBackFactory.registerDataCallBack(
      ExternalRelation("JDBC_SINK", tag, relationAlias, relationDDL)
    )
  }

}
