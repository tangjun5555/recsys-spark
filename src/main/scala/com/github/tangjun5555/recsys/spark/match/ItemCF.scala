package com.github.tangjun5555.recsys.spark.`match`

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer

/**
 * author: tangj 1844250138@qq.com
 * time: 2020/1/2 18:35
 * description: 基于物品的协同过滤
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
 * 1、物品时效性不高的推荐场景，如电商、广告推荐等
 *
 * 运行效率优化:
 * 1、构建共现对
 * 2、增量计算
 *
 */
class ItemCF extends I2IMatch with U2IMatch {

  private def shrinkDownSimilarity(similarity: Double, totalOccurrences: Int, lambda: Double): Double = {
    (similarity * totalOccurrences) / (totalOccurrences + lambda)
  }

  private var spark: SparkSession = _

  private var dataDF: DataFrame = _

  private var userColumnName: String = "user"

  private var itemColumnName: String = "item"

  private var ratingColumnName = "rating"

  /**
   * 是否是隐式反馈
   */
  private var implicitPrefs: Boolean = true

  /**
   * 用户最多正反馈物品数量
   * 控制物品共现矩阵的大小
   */
  private var maxUserRelatedItem: Int = 1000

  /**
   * 每个物品保留前N个相似度最高的物品
   */
  private var maxSimItemNum: Int = 20

  /**
   * 物品最小共现用户数量
   * 控制物品相似度计算精度
   */
  private var minCommonUserNum: Int = 5

  /**
   * 相似度校准参数
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

  def setMaxUserRelatedItem(value: Int): this.type = {
    this.maxUserRelatedItem = value
    this
  }

  def setMaxSimItemNum(value: Int): this.type = {
    this.maxSimItemNum = value
    this
  }

  def setImplicitPrefs(value: Boolean): this.type = {
    this.implicitPrefs = value
    this
  }

  def setMinCommonUserNum(value: Int): this.type = {
    this.minCommonUserNum = value
    this
  }

  def setShrinkDownSimilarityLambda(value: Double): this.type = {
    this.shrinkDownSimilarityLambda = value
    this
  }

  /**
   * 物品相似度计算结果
   */
  private var itemSimilarityDF: DataFrame = null

  def getItemSimilarityDF(): DataFrame = {
    this.itemSimilarityDF
  }

  def fit(rawDataDF: DataFrame): this.type = {
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

    // 统计物品的频率
    val itemFrequency: Map[String, Long] = dataDF
      .groupBy(itemColumnName)
      .agg(count(userColumnName).as(itemColumnName + "_frequency"))
      .rdd.map(row => (row.getAs[String](itemColumnName), row.getAs[Long](itemColumnName + "_frequency")))
      .collect().toMap
    println(s"[${this.getClass.getSimpleName}.fit] itemFrequency:${itemFrequency.slice(0, 10).mkString(",")}")
    val itemFrequencyBroadcast: Broadcast[Map[String, Long]] = spark.sparkContext.broadcast(itemFrequency)

    // 计算物品相似度
    this.itemSimilarityDF = if (implicitPrefs) {
      dataDF
        .rdd.map(row => (row.getAs[String](userColumnName), (row.getAs[String](itemColumnName), row.getAs[Double](ratingColumnName))))
        .groupByKey()
        .filter(_._2.size >= 2)
        .map(row => {
          // 避免产生过多的pair对
          if (row._2.size > maxUserRelatedItem) {
            println(s"[${this.getClass.getSimpleName}.fit], user:${row._1}, itemSize:${row._2.size}")
            val itemFrequencyValue = itemFrequencyBroadcast.value
            row._2.toSeq.sortBy(_._1)
              .map(x => (x._1, x._2, itemFrequencyValue.getOrElse(x._1, Long.MaxValue)))
              .sortBy(_._3).map(x => (x._1, x._2))
              .slice(0, maxUserRelatedItem)
          } else {
            row._2.toSeq.sortBy(_._1)
          }
        })
        .flatMap(row => {
          val itemSet: Seq[(String, Double)] = row.sorted
          for {i <- 0.until(itemSet.size - 1); j <- (i + 1).until(itemSet.size)}
            yield ((itemSet(i)._1, itemSet(j)._1), 1)
        })
        .reduceByKey(_ + _)
        .filter(_._2 >= minCommonUserNum)
        .map(x => {
          val itemFrequencyValue: Map[String, Long] = itemFrequencyBroadcast.value
          (x._1._1
            , x._1._2
            , shrinkDownSimilarity(
            x._2 * 1.0 / (itemFrequencyValue(x._1._1) + itemFrequencyValue(x._1._2) - x._2)
            , x._2
            , shrinkDownSimilarityLambda)
          )
        })
        .flatMap(x => Seq(
          (x._1, (x._2, x._3)),
          (x._2, (x._1, x._3)))
        )
        .groupByKey()
        .flatMap(x => {
          x._2.toSeq.sortBy(_._2).reverse.slice(0, maxSimItemNum)
            .map(y => (x._1, y._1, y._2))
        })
        .toDF("i1", "i2", "sim")
        .persist(StorageLevel.MEMORY_AND_DISK)
    } else {
      dataDF
        .rdd.map(row => (row.getAs[String](userColumnName), (row.getAs[String](itemColumnName), row.getAs[Double](ratingColumnName))))
        .groupByKey()
        .filter(_._2.size >= 2)
        .map(row => {
          // 避免产生过多的pair对
          if (row._2.size > maxUserRelatedItem) {
            println(s"[${this.getClass.getSimpleName}.fit], user:${row._1}, itemSize:${row._2.size}")
            val itemFrequencyValue = itemFrequencyBroadcast.value
            row._2.toSeq.sortBy(_._1)
              .map(x => (x._1, x._2, itemFrequencyValue.getOrElse(x._1, Long.MaxValue)))
              .sortBy(_._3).map(x => (x._1, x._2))
              .slice(0, maxUserRelatedItem)
          } else {
            row._2.toSeq.sortBy(_._1)
          }
        })
        .flatMap(row => {
          val buffer = ArrayBuffer[((String, String), (Double, Double, Double, Int))]()
          val itemSet: Seq[(String, Double)] = row.sorted
          for (i <- 0.until(itemSet.size - 1)) {
            for (j <- (i + 1).until(itemSet.size)) {
              buffer.+=(
                (
                  (itemSet(i)._1, itemSet(j)._1),
                  (itemSet(i)._2 * itemSet(j)._2,
                    math.pow(itemSet(i)._2, 2.0),
                    math.pow(itemSet(j)._2, 2.0),
                    1)
                )
              )
            }
          }
          buffer
        })
        .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3, x._4 + y._4))
        .filter(_._2._4 >= minCommonUserNum)
        .map(x => {
          val middleScore: (Double, Double, Double, Int) = x._2
          (x._1._1
            , x._1._2
            , shrinkDownSimilarity(
            middleScore._1 / math.sqrt(middleScore._2 * middleScore._3)
            , middleScore._4
            , shrinkDownSimilarityLambda)
          )
        })
        .flatMap(x => Seq(
          (x._1, (x._2, x._3)),
          (x._2, (x._1, x._3)))
        )
        .groupByKey()
        .flatMap(x => {
          x._2.toSeq.sortBy(_._2).reverse.slice(0, maxSimItemNum)
            .map(y => (x._1, y._1, y._2))
        })
        .toDF("i1", "i2", "sim")
        .persist(StorageLevel.MEMORY_AND_DISK)
    }

    println(s"[${this.getClass.getSimpleName}.fit] this.itemSimilarityDF.size:${this.itemSimilarityDF.count()}")
    this.itemSimilarityDF.show(30, false)

    this
  }

  override def recommendForUser(
                                 recNum: Int = 50
                                 , withScore: Boolean = false
                                 , recResultColumnName: String = "rec_items"
                               ): DataFrame = {
    val spark = this.spark
    import spark.implicits._

    dataDF.withColumnRenamed(itemColumnName, "i1")
      .join(this.itemSimilarityDF, Seq("i1"), "inner")
      .drop("i1")
      .withColumnRenamed("i2", itemColumnName)
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

  override def recommendForItem(
                                 recNum: Int = 50
                                 , withScore: Boolean = false
                                 , recResultColumnName: String = "rec_items"
                               ): DataFrame = {
    val spark = this.spark
    import spark.implicits._

    this.itemSimilarityDF.rdd
      .map(row => (
        row.getAs[String]("i1"),
        (row.getAs[String]("i2"), row.getAs[Double]("sim"))
      ))
      .groupByKey()
      .map(row => {
        val recItems = row._2.toSeq.sortBy(_._2).reverse.slice(0, recNum).map(x => {
          if (withScore) {
            x._1 + ":" + x._2.formatted("%.3f")
          } else {
            x._1
          }
        }).mkString(",")
        (row._1, recItems)
      })
      .toDF(this.itemColumnName, recResultColumnName)
  }

}
