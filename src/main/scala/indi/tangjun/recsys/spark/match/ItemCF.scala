package indi.tangjun.recsys.spark.`match`

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer

/**
 * author: tangj
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
 * 相似度算法的选择:
 * 1、Jaccard, 适用于只有只关注一种正向反馈行为的场景，如信息流点击等, rating只取1.0
 * 2、cosine, 适用于评分等级的场景, rating最小值设置1.0
 *
 * 适用场景:
 * 1、物品时效性不高的推荐场景，如电商、广告推荐等
 *
 * TODO List:
 * 1、热门物品、冷门物品的处理
 *
 */
class ItemCF extends I2IMatch with U2IMatch {

  private def shrinkDownSimilarity(similarity: Double, totalOccurrences: Int, lambda: Double): Double = {
    (similarity * totalOccurrences) / (totalOccurrences + lambda)
  }

  private var spark: SparkSession = _

  private def setSpark(spark: SparkSession): ItemCF = {
    this.spark = spark
    this
  }

  /**
   * 保留训练数据
   */
  private var dataDF: DataFrame = _

  private def setDataDF(dataDF: DataFrame): ItemCF = {
    this.dataDF = dataDF
    this
  }

  private var userColumnName: String = "user"

  def setUserColumnName(userColumnName: String): ItemCF = {
    this.userColumnName = userColumnName
    this
  }

  def getUserColumnName(): String = {
    this.userColumnName
  }

  private var itemColumnName: String = "item"

  def setItemColumnName(itemColumnName: String): ItemCF = {
    this.itemColumnName = itemColumnName
    this
  }

  def getItemColumnName(): String = {
    this.itemColumnName
  }

  private var ratingColumnName = "rating"

  def setRatingColumnName(ratingColumnName: String): ItemCF = {
    this.ratingColumnName = ratingColumnName
    this
  }

  def getRatingColumnName(): String = {
    this.ratingColumnName
  }

  /**
   * 控制物品共现矩阵的大小
   */
  private var maxUserRelatedItem: Int = 10000

  def setMaxUserRelatedItem(maxUserRelatedItem: Int): ItemCF = {
    this.maxUserRelatedItem = maxUserRelatedItem
    this
  }

  def getMaxUserRelatedItem(): Int = {
    this.maxUserRelatedItem
  }

  /**
   * 每个物品保留前N个相似度最高的物品
   */
  private var maxSimItemNum: Int = 20

  def setMaxSimItemNum(maxSimItemNum: Int): ItemCF = {
    this.maxSimItemNum = maxSimItemNum
    this
  }

  def getMaxSimItemNum(): Int = {
    this.maxSimItemNum
  }

  /**
   * 是否是隐式反馈
   */
  private var implicitPrefs: Boolean = true

  def setImplicitPrefs(implicitPrefs: Boolean): ItemCF = {
    this.implicitPrefs = implicitPrefs
    this
  }

  def getImplicitPrefs(): Boolean = {
    this.implicitPrefs
  }

  private var minCommonUserNum: Int = 5

  def setMinCommonUserNum(minCommonUserNum: Int): ItemCF = {
    this.minCommonUserNum = minCommonUserNum
    this
  }

  def getMinCommonUserNum(): Int = {
    this.minCommonUserNum
  }

  /**
   * 相似度校准
   */
  private var shrinkDownSimilarityLambda: Double = 50.0

  def setShrinkDownSimilarityLambda(shrinkDownSimilarityLambda: Double): ItemCF = {
    this.shrinkDownSimilarityLambda = shrinkDownSimilarityLambda
    this
  }

  def getShrinkDownSimilarityLambda(): Double = {
    this.shrinkDownSimilarityLambda
  }

  /**
   * 物品相似度计算结果
   */
  private var itemSimilarityDF: DataFrame = _

  def setItemSimilarityDF(itemSimilarityDF: DataFrame): ItemCF = {
    this.itemSimilarityDF = itemSimilarityDF
    this
  }

  def fit(rawDataDF: DataFrame
         ): ItemCF = {
    val spark = rawDataDF.sparkSession
    setSpark(spark)
    import spark.implicits._

    val dataDF = rawDataDF
      .groupBy(userColumnName, itemColumnName)
      .agg(max(ratingColumnName).as(ratingColumnName))
      .persist(StorageLevel.MEMORY_AND_DISK)
    setDataDF(dataDF)

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

    // 通过user倒排索引计算item pair
    val coCccurrenceItemPairRDD: RDD[((String, String), (Double, Double, Double, Int))] = dataDF
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
        val itemSet = row.sorted
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

    // 计算物品相似度
    val itemSimilarityDF = coCccurrenceItemPairRDD
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3, x._4 + y._4))
      .filter(_._2._4 >= minCommonUserNum)
      .map(x => {
        val middleScore = x._2
        if (implicitPrefs) {
          val itemFrequencyValue: Map[String, Long] = itemFrequencyBroadcast.value
          (x._1._1
            , x._1._2
            , shrinkDownSimilarity(
            middleScore._4 * 1.0 / (itemFrequencyValue(x._1._1) + itemFrequencyValue(x._1._2) - middleScore._4)
            , middleScore._4
            , shrinkDownSimilarityLambda)
          )
        } else {
          (x._1._1
            , x._1._2
            , shrinkDownSimilarity(
            middleScore._1 / math.sqrt(middleScore._2 * middleScore._3)
            , middleScore._4
            , shrinkDownSimilarityLambda)
          )
        }
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

    setItemSimilarityDF(itemSimilarityDF)
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
