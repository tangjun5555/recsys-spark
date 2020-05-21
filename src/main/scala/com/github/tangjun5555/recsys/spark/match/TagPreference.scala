package com.github.tangjun5555.recsys.spark.`match`

import com.github.tangjun5555.recsys.spark.util.ConcatIdByRank
import org.apache.spark.sql.functions.{col, max, sum}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
 * author: tangj
 * time: 2020/1/15 14:21
 * description: 基于标签偏好的推荐召回
 *
 * 应用场景:
 * 1、信息流（资讯、短视频）推荐中关于主题、作者、关键词标签的推荐召回
 * 2、电商购物推荐中关于类目的推荐召回
 * 3、广告推荐中关于素材类别（游戏、教育等）的推荐召回
 *
 * 时间衰减函数
 * a = math.exp(-0.1 * d) d为距离今天的天数
 * w = a * a
 */
class TagPreference extends U2IMatch {

  private var spark: SparkSession = _

  private var dataDF: DataFrame = _

  private var userColumnName: String = "user"

  private var itemColumnName: String = "item"

  private var ratingColumnName: String = "rating"

  private var timestampColumnName = "timestamp"

  private var tagColumnName: String = "tag"

  /**
   * 物品标签属性
   * item, tag
   */
  private var itemTagDF: DataFrame = null

  /**
   * 标签倒排索引
   * tag, item, tagScore
   */
  private var tagInvertedIndexDF: DataFrame = null

  def setUserColumnName(value: String): this.type = {
    this.userColumnName = value
    this
  }

  def setItemColumnName(value: String): this.type = {
    this.itemColumnName = value
    this
  }

  def setRatingColumnName(value: String): this.type = {
    this.ratingColumnName = value
    this
  }

  def setTagColumnName(value: String): this.type = {
    this.tagColumnName = value
    this
  }

  def setTagInvertedIndexDF(value: DataFrame): this.type = {
    this.tagInvertedIndexDF = value
    this
  }

  private var userTagScoreDF: DataFrame = null

  def fit(rawDataDF: DataFrame): this.type = {
    this.spark = rawDataDF.sparkSession

    this.dataDF = rawDataDF
      .groupBy(userColumnName, itemColumnName)
      .agg(max(ratingColumnName).as(ratingColumnName))
      .persist(StorageLevel.MEMORY_AND_DISK)

    // 统计基本信息
    println(s"[${this.getClass.getSimpleName}.fit] dataDF.size:${dataDF.count()}")
    println(s"[${this.getClass.getSimpleName}.fit] dataDF.user.size:${dataDF.select(userColumnName).distinct().count()}")
    println(s"[${this.getClass.getSimpleName}.fit] dataDF.item.size:${dataDF.select(itemColumnName).distinct().count()}")

    this.userTagScoreDF = dataDF.join(itemTagDF, Seq(itemColumnName), "inner")
      .groupBy(userColumnName, tagColumnName)
      .agg(sum(ratingColumnName).as("preferenceScore"))

    this
  }

  override def recommendForUser(recNum: Int, withScore: Boolean, recResultColumnName: String): DataFrame = {
    val spark = this.spark
    spark.udf.register("concatIdByRank", ConcatIdByRank)

    val finalScoreDF = this.userTagScoreDF.join(tagInvertedIndexDF, Seq(tagColumnName), "inner")
      .groupBy(userColumnName, itemColumnName)
      .agg(sum(col("preferenceScore") * col("tagScore")).as("final_score"))
    finalScoreDF.createTempView("finalScoreDF")

    spark.sql(
      s"""
         |select ${userColumnName}
         |  , concatIdByRank(${itemColumnName}, rank) rec_items
         |from
         |(
         |select ${userColumnName}
         |  , ${itemColumnName}
         |  , row_number() over(partition by ${userColumnName} order by final_score desc) rank
         |from finalScoreDF
         |) a
         |where rank<=${recNum}
         |group by ${userColumnName}
         |""".stripMargin)
  }

}
