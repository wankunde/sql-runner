// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package org.apache.spark.sql.udf

import java.lang.annotation.Annotation

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.util.Logging

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2020-07-20.
 */
object UDFFactory extends Logging {

  val EXTERNAL_UDFS = "spark.sql.externalUdfClasses"

  def registerExternalUDFs(spark: SparkSession): Unit = {
    spark.udf.register("transform_date", DateFormatUDF.transform_date_udf)

    Configuration.getOption(EXTERNAL_UDFS).map {
      case udfClasses: String =>
        spark.sessionState.resourceLoader.addJar("hdfs:///deploy/config/biz-udfs-1.0.jar")

        val annotationClazz =
          Class.forName("org.apachetech.udfs.annotations.UDFDescription",
            true,
            spark.sharedState.jarClassLoader)
            .asInstanceOf[Class[_ <: Annotation]]
        val nameMethod = annotationClazz.getMethod("name")
        val returnTypeMethod = annotationClazz.getMethod("returnType")

        udfClasses.split(",").map(_.trim).foreach(udfClass => {
          val clazz = Class.forName(udfClass, true, spark.sharedState.jarClassLoader)
          val annotation = clazz.getAnnotation(annotationClazz)
          val name: String = nameMethod.invoke(annotation).asInstanceOf[String]
          val returnType: String = returnTypeMethod.invoke(annotation).asInstanceOf[String]

          logInfo(s"register udf ${name} with class ${udfClass}")
          spark.udf.registerJava(name, udfClass, DataType.fromDDL(returnType))
        })
    }
  }
}
