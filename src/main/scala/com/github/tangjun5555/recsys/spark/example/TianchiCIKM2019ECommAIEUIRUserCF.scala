package com.github.tangjun5555.recsys.spark.example

import com.github.tangjun5555.recsys.spark.`match`.UserCF
import com.github.tangjun5555.recsys.spark.jutil.MathFunctionUtil
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
object TianchiCIKM2019ECommAIEUIRUserCF {

  def main(args: Array[String]): Unit = {
    val spark = SparkUtil.getSparkSession(this.getClass.getSimpleName, cores = 5)
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
         |select distinct user_id, item_id, 1.0d rating
         |from trainDataDF
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

    val model = new UserCF()
      .setUserColumnName("user_id")
      .setItemColumnName("item_id")
      .setRatingColumnName("rating")
      .setImplicitPrefs(true)
      .setMaxItemRelatedUser(10 * 10000)
      .fit(modelTrainDF)
    val predictDF = model.recommendForUser()

    val r1 = modelValidDF.join(predictDF, Seq("user_id"), "inner")
      .select("user_id", "labels", "rec_items")
      .persist(StorageLevel.MEMORY_AND_DISK)
    r1.show(10, false)
    val r1Count = r1.count() * 1.0
    println(s"[${this.getClass.getSimpleName}] 算法覆盖度:${(r1.count() * 1.0 / modelValidDF.count())}")

    import scala.collection.JavaConverters._
    val r2 = r1
      .rdd.map(row =>
      (row.getAs[String]("user_id"), row.getAs[String]("labels"), row.getAs[String]("rec_items"))
    )
      .map(row => (row._1, row._2.length, row._3.length
        , new java.util.ArrayList[String](row._2.split(",").toSeq.asJava)
        , new java.util.ArrayList[String](row._3.split(",").toSeq.asJava)
      ))
      .persist(StorageLevel.MEMORY_AND_DISK)

    println(s"[${this.getClass.getSimpleName}] Recall@1:${String.format("%.4f", java.lang.Double.valueOf(r2.map(x => MathFunctionUtil.computeRecRecallRate(x._4, x._5, 1)).reduce(_ + _) / r1Count))}")
    println(s"[${this.getClass.getSimpleName}] Recall@5:${String.format("%.4f", java.lang.Double.valueOf(r2.map(x => MathFunctionUtil.computeRecRecallRate(x._4, x._5, 5)).reduce(_ + _) / r1Count))}")
    println(s"[${this.getClass.getSimpleName}] Recall@10:${String.format("%.4f", java.lang.Double.valueOf(r2.map(x => MathFunctionUtil.computeRecRecallRate(x._4, x._5, 10)).reduce(_ + _) / r1Count))}")
    println(s"[${this.getClass.getSimpleName}] Recall@30:${String.format("%.4f", java.lang.Double.valueOf(r2.map(x => MathFunctionUtil.computeRecRecallRate(x._4, x._5, 30)).reduce(_ + _) / r1Count))}")
    println(s"[${this.getClass.getSimpleName}] Recall@50:${String.format("%.4f", java.lang.Double.valueOf(r2.map(x => MathFunctionUtil.computeRecRecallRate(x._4, x._5, 50)).reduce(_ + _) / r1Count))}")

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

}
