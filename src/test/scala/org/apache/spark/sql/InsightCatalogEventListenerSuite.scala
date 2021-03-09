// Copyright 2019 Leyantech Ltd. All Rights Reserved.
package org.apache.spark

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{InsightCatalogEventListener, SparkSqlRunnerBase}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.hive.HiveExternalCatalog.STATISTICS_NUM_ROWS

import scala.collection.mutable

/**
 * @author kun.wan, <kun.wan@leyantech.com>
 * @date 2020-05-06.
 */
class InsightCatalogEventListenerSuite extends SparkSqlRunnerBase {

  protected val utils = new CatalogTestUtils {
    override val tableInputFormat: String = "com.fruit.eyephone.CameraInputFormat"
    override val tableOutputFormat: String = "com.fruit.eyephone.CameraOutputFormat"
    override val defaultProvider: String = "parquet"

    override def newEmptyCatalog(): ExternalCatalog = new InMemoryCatalog
  }

  override def beforeAll(): Unit = {
    super.beforeAll()

    cleanTestData(Seq("/test-datas/part_table"))
    spark.sql("CREATE TABLE part_table(id int) PARTITIONED BY (`dt` string)")
  }

  override def afterAll(): Unit = {
    cleanTestData(Seq("/test-datas/part_table"))

    super.afterAll()
  }

  def cleanTestData(resourcePaths: Seq[String]): Unit = {
    resourcePaths.map { resourcePath =>
      val url = getClass.getResource(resourcePath)
      if (url != null) {
        val deleteFile = new File(url.getFile)
        if (deleteFile.exists()) {
          FileUtils.forceDelete(deleteFile)
        }
      }
    }
  }

  test("test bollingerCheck with mock partitions") {
    val listener: InsightCatalogEventListener = InsightCatalogEventListener()
    val now = System.currentTimeMillis
    val partitions = Range(1, 20).map { i =>
      CatalogTablePartition(
        Map("dt" -> s"${20200500 + i}"),
        utils.storageFormat,
        parameters = Map(STATISTICS_NUM_ROWS -> s"${100 + i}"),
        createTime = now + i * 10
      )
    }

    listener.partitionRowNumberCheck(
      TableIdentifier("t"),
      Map("dt" -> "20200519"),
      partitions
    )

    //    InsightCatalogEventListener().bollingerCheck(
    //      TableIdentifier("t"),
    //      Map("dt" -> "20200520"),
    //      mutable.Buffer() ++= partitions +=
    //        CatalogTablePartition(
    //          Map("dt" -> "20200520"),
    //          utils.storageFormat,
    //          parameters = Map(STATISTICS_NUM_ROWS -> "200"),
    //          createTime = now + 200
    //        )
    //    )
  }

  val testdata1 = Seq(
    Tuple2("20200501", 6479),
    Tuple2("20200502", 7168),
    Tuple2("20200503", 7182),
    Tuple2("20200504", 7175),
    Tuple2("20200505", 10290),
    Tuple2("20200506", 10615),
    Tuple2("20200507", 7221),
    Tuple2("20200508", 6673),
    Tuple2("20200509", 7301),
    Tuple2("20200510", 13261),
    Tuple2("20200511", 13737),
    Tuple2("20200512", 135664),
    Tuple2("20200513", 510327),
    Tuple2("20200514", 449180),
    Tuple2("20200515", 455229),
    Tuple2("20200516", 480892),
    Tuple2("20200517", 542436)
  )

  val testdata2 = Seq(
    Tuple2("20200501", 275),
    Tuple2("20200502", 285),
    Tuple2("20200503", 285),
    Tuple2("20200504", 298),
    Tuple2("20200505", 314),
    Tuple2("20200506", 323),
    Tuple2("20200507", 332),
    Tuple2("20200508", 350),
    Tuple2("20200509", 356),
    Tuple2("20200510", 358),
    Tuple2("20200511", 379),
    Tuple2("20200512", 393),
    Tuple2("20200513", 399),
    Tuple2("20200514", 409),
    Tuple2("20200515", 474),
    Tuple2("20200516", 493),
    Tuple2("20200517", 508),
    Tuple2("20200518", 550),
    Tuple2("20200519", 353)
  )

  val testdata3 = Seq(
    Tuple2("20200501", 76006),
    Tuple2("20200502", 72343),
    Tuple2("20200503", 68919),
    Tuple2("20200504", 67308),
    Tuple2("20200505", 73862),
    Tuple2("20200506", 70709),
    Tuple2("20200507", 68437),
    Tuple2("20200508", 63599),
    Tuple2("20200509", 68659),
    Tuple2("20200510", 82189),
    Tuple2("20200511", 74408),
    Tuple2("20200512", 81384),
    Tuple2("20200513", 89102),
    Tuple2("20200514", 78057),
    Tuple2("20200515", 98009),
    Tuple2("20200516", 113368),
    Tuple2("20200517", 135421),
    Tuple2("20200518", 114899),
    Tuple2("20200519", 48333)
  )

  test("test and compare model") {
    val listener: InsightCatalogEventListener = InsightCatalogEventListener()
    Seq(testdata1, testdata2, testdata3).foreach { rowNums =>
      logInfo("begin new compare")
      // bollinger model
      for (i <- Range(10, rowNums.length)) {
        val dt = rowNums(i)._1
        val sampleRowNums = rowNums.take(i).drop(i - 10).map(_._2)
        val (ma, up, dn) = listener.bollingerStat(sampleRowNums)
        logInfo(s"${dt}\t${ma}\t${up}\t${dn}")
      }

      // ewma model
      for (i <- Range(10, rowNums.length)) {
        val dt = rowNums(i)._1
        val sampleRowNums = rowNums.take(i).drop(i - 10).map(_._2)
        val (ma, up, dn) = listener.ewmaStat(sampleRowNums)
        logInfo(s"${dt}\t${ma}\t${up}\t${dn}")
      }
    }
  }

  test("test partition event") {
    import spark.implicits._

    val listener = InsightCatalogEventListener()
    spark.sharedState.externalCatalog.addListener(listener)
    spark.sparkContext.parallelize(Range(1, 10)).toDF()
      .createOrReplaceTempView("tab")
    spark.sql(
      s"""
         |INSERT OVERWRITE part_table partition(dt='part1')
         |SELECT * FROM tab
         |""".stripMargin)
    spark.sql("SELECT * FROM part_table").show()
  }
}
