package com.github.tangjun5555.recsys.spark.`match`

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer

/**
 * author: tangj 1844250138@qq.com
 * time: 2020/1/2 18:36
 * description: 基于用户协同过滤的召回 U2U2I
 *
 * implicitPrefs
 * 1、true 隐性反馈
 * 如电商购买、信息流点击等, rating只取一个值, 如1.0
 * 相似算法选择jaccard
 * 2、false 显性反馈
 * 用户的真实评分, 如豆瓣评分、电商购物评分
 * 相似算法选择cosine
 *
 * 适用场景:
 * 1、物品时效性较高的推荐场景，如资讯信息流、视频信息流
 *
 * 运行效率优化:
 * 1、构建共现对
 * 2、增量计算
 *
 */
class UserCF extends U2IMatch {

  private def shrinkDownSimilarity(similarity: Double, totalOccurrences: Int, lambda: Double): Double = {
    (similarity * totalOccurrences) / (totalOccurrences + lambda)
  }

  private var spark: SparkSession = _

  /**
   * 保留训练数据
   */
  private var dataDF: DataFrame = _

  private var userColumnName: String = "user"

  private var itemColumnName: String = "item"

  private var ratingColumnName = "rating"

  /**
   * 控制用户共现矩阵的大小
   */
  private var maxItemRelatedUser: Int = 10000

  /**
   * 每个用户保留相似度最高前N的用户
   */
  private var maxSimUserNum: Int = 20

  /**
   * 是否是隐式反馈
   */
  private var implicitPrefs: Boolean = true

  private var minCommonItemNum: Int = 5

  /**
   * 相似度校准
   */
  private var shrinkDownSimilarityLambda: Double = 50.0

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

  def setMaxItemRelatedUser(value: Int): this.type = {
    this.maxItemRelatedUser = value
    this
  }

  def setMaxSimUserNum(value: Int): this.type = {
    this.maxSimUserNum = value
    this
  }

  def setImplicitPrefs(value: Boolean): this.type = {
    this.implicitPrefs = value
    this
  }

  def setMinCommonItemNum(value: Int): this.type = {
    this.minCommonItemNum = value
    this
  }

  def setShrinkDownSimilarityLambda(value: Double): this.type = {
    this.shrinkDownSimilarityLambda = value
    this
  }

  /**
   * 用户相似矩阵
   */
  private var userSimilarityDF: DataFrame = _

  def fit(rawDataDF: DataFrame): UserCF = {
    val spark = rawDataDF.sparkSession
    this.spark = spark
    import spark.implicits._

    this.dataDF = rawDataDF
      .groupBy(userColumnName, itemColumnName)
      .agg(max(ratingColumnName).as(ratingColumnName))
      .persist(StorageLevel.MEMORY_AND_DISK)

    // 统计基本信息
    println(s"[${this.getClass.getSimpleName}.fit] dataDF.size:${dataDF.count()}")
    println(s"[${this.getClass.getSimpleName}.fit] dataDF.user.size:${dataDF.select(userColumnName).distinct().count()}")
    println(s"[${this.getClass.getSimpleName}.fit] dataDF.item.size:${dataDF.select(itemColumnName).distinct().count()}")

    // 统计用户的频率
    val userFrequency = dataDF.groupBy(userColumnName)
      .agg(count(itemColumnName).as(userColumnName + "_frequency"))
      .rdd.map(row => (row.getAs[String](userColumnName), row.getAs[Long](userColumnName + "_frequency")))
      .collect().toMap
    println(s"[${this.getClass.getSimpleName}.fit] userFrequency:${userFrequency.slice(0, 10).mkString(",")}")
    val userFrequencyBroadcast: Broadcast[Map[String, Long]] = spark.sparkContext.broadcast(userFrequency)

    // 计算用户相似度
    this.userSimilarityDF = dataDF
      .rdd.map(row => (row.getAs[String](itemColumnName), (row.getAs[String](userColumnName), row.getAs[Double](ratingColumnName))))
      .groupByKey()
      .filter(_._2.size >= 2)
      .map(row => {
        if (row._2.size > maxItemRelatedUser) {
          println(s"[${this.getClass.getSimpleName}.fit], item:${row._1}, userSize:${row._2.size}")
          val userFrequencyValue = userFrequencyBroadcast.value
          row._2.toSeq.sortBy(_._1)
            .map(x => (x._1, x._2, userFrequencyValue.getOrElse(x._1, Long.MaxValue)))
            .sortBy(_._3).map(x => (x._1, x._2))
            .slice(0, maxItemRelatedUser)
        } else {
          row._2.toSeq.sortBy(_._1)
        }
      })
      .flatMap(row => {
        val buffer = ArrayBuffer[((String, String), (Double, Double, Double, Int))]()
        val itemSet = row.sorted
        for (i <- 0.until(itemSet.size - 1)) {
          for (j <- (i + 1).until(itemSet.size)) {
            buffer.+=(
              (
                (itemSet(i)._1, itemSet(j)._1)
                , (itemSet(i)._2 * itemSet(j)._2, math.pow(itemSet(i)._2, 2.0), math.pow(itemSet(j)._2, 2.0), 1))
            )
          }
        }
        buffer
      })
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3, x._4 + y._4))
      .filter(_._2._4 >= minCommonItemNum)
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3, x._4 + y._4))
      .map(x => {
        val middleScore = x._2
        if (implicitPrefs) {
          val userFrequencyValue = userFrequencyBroadcast.value
          (x._1._1
            , x._1._2
            , shrinkDownSimilarity(
            middleScore._4 * 1.0 / (userFrequencyValue(x._1._1) + userFrequencyValue(x._1._2) - middleScore._4)
            , middleScore._4
            , shrinkDownSimilarityLambda
          ))
        } else {
          (x._1._1
            , x._1._2
            , shrinkDownSimilarity(
            middleScore._1 / math.sqrt(middleScore._2 * middleScore._3)
            , middleScore._4
            , shrinkDownSimilarityLambda
          ))
        }
      })
      .flatMap(x => Seq(
        (x._1, (x._2, x._3)),
        (x._2, (x._1, x._3)))
      )
      .groupByKey()
      .flatMap(x => {
        x._2.toSeq.sortBy(_._2).reverse.slice(0, maxSimUserNum)
          .map(y => (x._1, y._1, y._2))
      })
      .toDF("u1", "u2", "sim")
      .persist(StorageLevel.MEMORY_AND_DISK)

    println(s"[${this.getClass.getSimpleName}.fit] this.userSimilarityDF.size:${userSimilarityDF.count()}")
    this.userSimilarityDF.show(30, false)

    this
  }

  override def recommendForUser(
                                 recNum: Int = 50
                                 , withScore: Boolean = false
                                 , recResultColumnName: String = "rec_items"
                               ): DataFrame = {
    val spark = this.spark
    import spark.implicits._

    this.userSimilarityDF.withColumnRenamed("u2", userColumnName)
      .join(dataDF, Seq(userColumnName), "inner")
      .drop(userColumnName)
      .withColumnRenamed("u1", userColumnName)
      .rdd.map(row => ((row.getAs[String](userColumnName), row.getAs[String](itemColumnName)), row.getAs[Double](ratingColumnName) * row.getAs[Double]("sim")))
      .reduceByKey(_ + _)
      .map(row => (row._1._1, (row._1._2, row._2)))
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
