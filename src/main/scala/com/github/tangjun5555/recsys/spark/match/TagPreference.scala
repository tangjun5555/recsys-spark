package com.github.tangjun5555.recsys.spark.`match`

import org.apache.spark.sql.functions.{col, sum}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
 * author: tangj 1844250138@qq.com
 * time: 2020/1/15 14:21
 * description: 基于用户标签偏好的召回 U2T2I
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

  private var userColumnName: String = "user"

  private var itemColumnName: String = "item"

  private var ratingColumnName = "rating"

  private var tagColumnName: String = "tag"

  private var scoreColumnName: String = "score"

  /**
   * 用户对物品的评分
   * user item score
   */
  private var userRatingDF: DataFrame = _

  /**
   * 物品的标签优势得分
   * item tag score
   */
  private var itemTagScoreDF: DataFrame = _

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

  def setScoreColumnName(value: String): this.type = {
    this.scoreColumnName = value
    this
  }

  def setItemTagScoreDF(value: DataFrame): this.type = {
    this.itemTagScoreDF = value.persist(StorageLevel.MEMORY_AND_DISK)
    this
  }

  private var userTagScoreDF: DataFrame = _

  def fit(rawDataDF: DataFrame): this.type = {
    this.spark = rawDataDF.sparkSession

    userRatingDF = rawDataDF
      .persist(StorageLevel.MEMORY_AND_DISK)

    // 统计基本信息
    println(s"[${this.getClass.getSimpleName}.fit] userRatingDF.size:${userRatingDF.count()}")
    println(s"[${this.getClass.getSimpleName}.fit] userRatingDF.user.size:${userRatingDF.select(userColumnName).distinct().count()}")
    println(s"[${this.getClass.getSimpleName}.fit] userRatingDF.item.size:${userRatingDF.select(itemColumnName).distinct().count()}")

    this.userTagScoreDF = userRatingDF.join(itemTagScoreDF, Seq(itemColumnName), "inner")
      .groupBy(userColumnName, tagColumnName)
      .agg(sum(ratingColumnName).as("preferenceScore"))
      .persist(StorageLevel.MEMORY_AND_DISK)
    println(s"[${this.getClass.getSimpleName}.fit] userTagScoreDF.count:${userTagScoreDF.count()}")
    userTagScoreDF.show(50, false)

    this
  }

  override def recommendForUser(recNum: Int = 50
                                , withScore: Boolean = false
                                , recResultColumnName: String = "rec_items"): DataFrame = {
    val spark = this.spark
    import spark.implicits._

    this.userTagScoreDF.join(itemTagScoreDF, Seq(tagColumnName), "inner")
      .groupBy(userColumnName, itemColumnName)
      .agg(sum(col("preferenceScore") * col("tagScore")).as("final_score"))
      .rdd
      .map(row => (row.getAs[String](userColumnName), (row.getAs[String](itemColumnName), row.getAs[Double]("final_score"))))
      .groupByKey()
      .map(row => (row._1,
        row._2.toSeq.sortBy(_._2).reverse.slice(0, recNum)
          .map(x => {
            if (withScore) {
              x._1 + ":" + x._2.formatted("%.3f")
            } else {
              x._1
            }
          })
          .mkString(",")
      ))
      .toDF(userColumnName, recResultColumnName)
  }

}
