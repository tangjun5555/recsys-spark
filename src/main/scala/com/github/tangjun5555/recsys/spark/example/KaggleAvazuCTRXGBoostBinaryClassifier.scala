package com.github.tangjun5555.recsys.spark.example

import com.github.tangjun5555.recsys.spark.jutil.JavaTimeUtil
import com.github.tangjun5555.recsys.spark.rank.XGBoostBinaryClassifier
import com.github.tangjun5555.recsys.spark.util.SparkUtil
import org.apache.log4j.Level
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.evaluation.AUCAndLogLossEvaluator
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
 * author: tangj 1844250138@qq.com
 * time: 2020/6/3 9:59 上午
 * description:
 *
 * 数据集:https://www.kaggle.com/c/avazu-ctr-prediction/data
 *
 * column name:
 * id,click,hour,C1,banner_pos,site_id,site_domain,site_category,app_id,app_domain,app_category,device_id,device_ip,device_model,device_type,device_conn_type,C14,C15,C16,C17,C18,C19,C20,C21
 *
 */
object KaggleAvazuCTRXGBoostBinaryClassifier {

  val labelColumnName = "click"
  val columnNames = "id,click,hour,C1,banner_pos,site_id,site_domain,site_category,app_id,app_domain,app_category,device_id,device_ip,device_model,device_type,device_conn_type,C14,C15,C16,C17,C18,C19,C20,C21"
    .split(",").map(_.toLowerCase)
  val featureNames = Seq("event_hour").++(columnNames.slice(3, columnNames.size))

  def main(args: Array[String]): Unit = {
    val startTime = JavaTimeUtil.getCurrentDateTime

    val rawDataFile = args(0)
    val modelSavePath = args(1)

    val spark = SparkUtil.getSparkSession(this.getClass.getSimpleName, cores = 5, logLevel = Level.WARN)
    import spark.implicits._

    spark.udf.register("getEventDate", getEventDate _)
    spark.udf.register("getEventHour", getEventHour _)
    spark.udf.register("transformCategory", transformCategory _)

    // 读取原始数据
    val schema: StructType = StructType(columnNames.map(x => StructField(x, StringType)))
    val rawDataDF: DataFrame = spark.read.schema(schema)
      .option("header", true.toString)
      .csv(rawDataFile)
      .drop("id")
      .persist(StorageLevel.MEMORY_AND_DISK)
    rawDataDF.createTempView("rawDataDF")
    rawDataDF.show(50, false)

    spark.sql(
      s"""
         |select click, count(hour) num
         |from rawDataDF
         |group by click
         |""".stripMargin
    ).show(50, false)

    val event_hour_enums: Seq[String] = 0.until(10).map(x => s"0${x}").++(10.until(24).map(_.toString))
    val featureEnums: Array[(String, Seq[String])] = Array(("event_hour", event_hour_enums))
      .++(
        columnNames.slice(3, columnNames.size).map(x => {
          (x, rawDataDF.select(x).distinct().rdd.map(_.getAs[String](0)).collect().sorted.toSeq)
        }))
    featureEnums.map(x => s"${x._1}, ${x._2.size}, ${x._2.mkString(",")}").foreach(println)
    val featureEnumsBroadcast: Broadcast[Array[(String, Seq[String])]] = spark.sparkContext.broadcast(featureEnums)

    val transformDataDF = spark.sql(
      s"""
         |select getEventDate(hour) event_date
         |  , getEventHour(hour) event_hour
         |  , *
         |  , transformCategory(getEventHour(hour), '${featureEnums.toMap.apply("event_hour")}') event_hour_new
         |  , transformCategory(c1, '${featureEnums.toMap.apply("c1").mkString(",")}') c1_new
         |  , transformCategory(banner_pos, '${featureEnums.toMap.apply("banner_pos").mkString(",")}') banner_pos_new
         |  , transformCategory(site_id, '${featureEnums.toMap.apply("site_id").mkString(",")}') site_id_new
         |  , transformCategory(site_domain, '${featureEnums.toMap.apply("site_domain").mkString(",")}') site_domain_new
         |  , transformCategory(site_category, '${featureEnums.toMap.apply("site_category").mkString(",")}') site_category_new
         |  , transformCategory(app_id, '${featureEnums.toMap.apply("app_id").mkString(",")}') app_id_new
         |  , transformCategory(app_domain, '${featureEnums.toMap.apply("app_domain").mkString(",")}') app_domain_new
         |  , transformCategory(app_category, '${featureEnums.toMap.apply("app_category").mkString(",")}') app_category_new
         |  , transformCategory(device_id, '${featureEnums.toMap.apply("device_id").mkString(",")}') device_id_new
         |  , transformCategory(device_ip, '${featureEnums.toMap.apply("device_ip").mkString(",")}') device_ip_new
         |  , transformCategory(device_model, '${featureEnums.toMap.apply("device_model").mkString(",")}') device_model_new
         |  , transformCategory(device_type, '${featureEnums.toMap.apply("device_type").mkString(",")}') device_type_new
         |  , transformCategory(device_conn_type, '${featureEnums.toMap.apply("device_conn_type").mkString(",")}') device_conn_type_new
         |  , transformCategory(c14, '${featureEnums.toMap.apply("c14").mkString(",")}') c14_new
         |  , transformCategory(c15, '${featureEnums.toMap.apply("c15").mkString(",")}') c15_new
         |  , transformCategory(c16, '${featureEnums.toMap.apply("c16").mkString(",")}') c16_new
         |  , transformCategory(c17, '${featureEnums.toMap.apply("c17").mkString(",")}') c17_new
         |  , transformCategory(c18, '${featureEnums.toMap.apply("c18").mkString(",")}') c18_new
         |  , transformCategory(c19, '${featureEnums.toMap.apply("c19").mkString(",")}') c19_new
         |  , transformCategory(c20, '${featureEnums.toMap.apply("c20").mkString(",")}') c20_new
         |  , transformCategory(c21, '${featureEnums.toMap.apply("c21").mkString(",")}') c21_new
         |from rawDataDF
         |""".stripMargin)
      .persist(StorageLevel.MEMORY_AND_DISK)
    transformDataDF.createTempView("transformDataDF")
    transformDataDF.show(50, false)
    rawDataDF.unpersist()

    val modelDataDF = transformDataDF.rdd.map(row => {
      val event_date = row.getAs[String]("event_date")
      val label: Double = row.getAs[String](labelColumnName).toDouble

      val values: Map[String, Int] = featureNames.map(x => {
        (x, row.getAs[Int](s"${x}_new"))
      })
        .toMap

      (event_date, label, combineAllFeature(values, featureEnumsBroadcast.value.map(x => (x._1, x._2.size + 1)).toMap))
    })
      .toDF("event_date", "label", "features")
      .persist(StorageLevel.MEMORY_AND_DISK)
    modelDataDF.createTempView("modelDataDF")
    modelDataDF.show(30, false)
    transformDataDF.unpersist()

    val trainDF = modelDataDF.filter(s"event_date < 14103022").select("label", "features").persist(StorageLevel.MEMORY_AND_DISK)
//    val validDF = modelDataDF.filter(s"event_date >= 14103022").select("label", "features").persist(StorageLevel.MEMORY_AND_DISK)

    val model = new XGBoostBinaryClassifier()
      .fit(trainDF)

    val trainPreDF = model.predict(trainDF)
//    val validPreDF = model.predict(validDF)

    val evaluator = new AUCAndLogLossEvaluator()
    val (trainLogLoss, trainAUC) = evaluator.evaluate(trainPreDF)
//    val (validLogLoss, validAUC) = evaluator.evaluate(validPreDF)

    println(s"trainLogLoss:${trainLogLoss}, trainAUC:${trainAUC}")
//    println(s"validLogLoss:${validLogLoss}, validAUC:${validAUC}")

    model.save(modelSavePath)

    val endTime = JavaTimeUtil.getCurrentDateTime
    println(s"startTime:${startTime}, endTime:${endTime}")
    Thread.sleep(1000 * 1000)
    spark.stop()
  }

  def getEventDate(hour: String): String = {
    hour.substring(0, 6)
  }

  def getEventHour(hour: String): String = {
    hour.substring(6, 8)
  }

  /**
   * 处理离散特征
   * 缺失和频率少是两种情况
   *
   * @param value
   * @param enums
   * @return
   */
  def transformCategory(value: String, enums: String): Int = {
    val realEnums: Array[String] = enums.split(",")
    var flag = true
    var result: Int = 0
    for (i <- realEnums.indices if (flag)) {
      if (realEnums(i).equals(value)) {
        result = 1 + i
        flag = false
      }
    }
    if (Random.nextInt(10000 * 10) == 0) {
      println("transformCategory", value, enums, result)
    }
    result
  }

  def combineAllFeature(values: Map[String, Int], vocabSizeMap: Map[String, Int]): org.apache.spark.ml.linalg.Vector = {
    var prefix = 0
    val features = ArrayBuffer[(Int, Double)]()

    for (x <- featureNames) {
      features.append(
        (prefix + values(x), 1.0)
      )
      prefix += vocabSizeMap(x)
    }

    if (Random.nextInt(10000 * 10) == 0) {
      println("combineAllFeature", values, vocabSizeMap, features)
    }
    org.apache.spark.ml.linalg.Vectors.sparse(vocabSizeMap.values.sum, features)
  }

}
