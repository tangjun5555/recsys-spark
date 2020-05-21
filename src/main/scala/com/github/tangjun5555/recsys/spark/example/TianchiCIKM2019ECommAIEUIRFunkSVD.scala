package com.github.tangjun5555.recsys.spark.example

import com.github.tangjun5555.recsys.spark.`match`.FunkSVD
import com.github.tangjun5555.recsys.spark.util.SparkUtil
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
 * author: tangj
 * time: 2020/5/16 19:14
 * description:
 *
 * 使用天池比赛数据测试
 * https://tianchi.aliyun.com/competition/entrance/231721/introduction
 *
 */
object TianchiCIKM2019ECommAIEUIRFunkSVD {

  def main(args: Array[String]): Unit = {
    val spark = SparkUtil.getSparkSession(this.getClass.getSimpleName, cores = 5)
    spark.udf.register("behaviorType2Rating", behaviorType2Rating _)

    val dataPrefix = args(0)
    val allDataDF = Seq("round1_train_user_behavior.csv", "round1_testA_user_behavior.csv", "round1_testB_user_behavior.csv", "round2_train_user_behavior.csv").map(x => {
      readFile(spark, dataPrefix + x)
    })
      .reduce(_.union(_))
      .persist(StorageLevel.MEMORY_AND_DISK)
    allDataDF.createTempView("allDataDF")

    spark.sql(
      s"""
         |select count(timestamp) n0
         |  , count(distinct user_id) n1
         |  , count(distinct item_id) n2
         |  , min(timestamp) n3
         |  , max(timestamp) n4
         |from allDataDF
         |""".stripMargin)
      .show(10, false)

    spark.sql(
      s"""
         |select behavior_type, count(user_id) num
         |from allDataDF
         |group by behavior_type
         |""".stripMargin)
      .show(10, false)

    val trainDataDF = allDataDF.filter("timestamp<=1209600")
    trainDataDF.createTempView("trainDataDF")

    val validDataDF = allDataDF.filter("timestamp>1209600")
    validDataDF.createTempView("validDataDF")

    val modelTrainDF = spark.sql(
      s"""
         |select user_id, item_id
         |  , max(behaviorType2Rating(behavior_type)) rating
         |from trainDataDF
         |group by user_id, item_id
         |""".stripMargin)

    val modelValidDF = spark.sql(
      s"""
         |select user_id
         |  , CONCAT_WS(',', collect_set(item_id)) labels
         |  , count(item_id) num
         |from validDataDF
         |group by user_id
         |""".stripMargin)
      .persist(StorageLevel.MEMORY_AND_DISK)
    modelValidDF.show(20, false)

    val model = new FunkSVD()
      .setUserColumnName("user_id")
      .setItemColumnName("item_id")
      .setRatingColumnName("rating")
      .fit(modelTrainDF)

    val userEmbedding = model.getUserEmbedding()

    val itemEmbedding = model.getItemEmbedding()

    spark.stop()
  }

  def readFile(spark: SparkSession, path: String): DataFrame = {
    import spark.implicits._
    spark.sparkContext.textFile(path)
      .map(line => {
        val items = line.trim.split(",")
        assert(items.size == 4)
        (items(0), items(1), items(2), items(3).toLong)
      })
      .toDF("user_id", "item_id", "behavior_type", "timestamp")
  }

  def behaviorType2Rating(behaviorType: String): Double = {
    if ("pv".equals(behaviorType)) {
      1.0
    } else if ("buy".equals(behaviorType)) {
      10.0
    } else {
      5.0
    }
  }

}
