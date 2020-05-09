package indi.tangjun.recsys.spark.`match`

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
 * author: tangj
 * time: 2020/1/2 18:36
 * description: 基于用户的协同过滤
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

  def setUserColumnName(userColumnName: String): UserCF = {
    this.userColumnName = userColumnName
    this
  }

  def getUserColumnName(): String = {
    this.userColumnName
  }

  private var itemColumnName: String = "item"

  def setItemColumnName(itemColumnName: String): UserCF = {
    this.itemColumnName = itemColumnName
    this
  }

  def getItemColumnName(): String = {
    this.itemColumnName
  }

  private var ratingColumnName = "rating"

  def setRatingColumnName(ratingColumnName: String): UserCF = {
    this.ratingColumnName = ratingColumnName
    this
  }

  def getRatingColumnName(): String = {
    this.ratingColumnName
  }

  /**
   * 控制用户共现矩阵的大小
   */
  private var maxItemRelatedUser: Int = 10000

  def setMaxItemRelatedUser(maxItemRelatedUser: Int): UserCF = {
    this.maxItemRelatedUser = maxItemRelatedUser
    this
  }

  def getMaxItemRelatedUser(): Int = {
    this.maxItemRelatedUser
  }

  /**
   * 每个用户保留相似度最高前N的用户
   */
  private var maxSimUserNum: Int = 20

  def setMaxSimUserNum(maxSimUserNum: Int): UserCF = {
    this.maxSimUserNum = maxSimUserNum
    this
  }

  def getMaxSimUserNum(): Int = {
    this.maxSimUserNum
  }

  /**
   * 是否是隐式反馈
   */
  private var implicitPrefs: Boolean = true

  def setImplicitPrefs(implicitPrefs: Boolean): UserCF = {
    this.implicitPrefs = implicitPrefs
    this
  }

  def getImplicitPrefs(): Boolean = {
    this.implicitPrefs
  }

  private var minCommonItemNum: Int = 5

  def setMinCommonItemNum(minCommonItemNum: Int): UserCF = {
    this.minCommonItemNum = minCommonItemNum
    this
  }

  def getMinCommonItemNum(): Int = {
    this.minCommonItemNum
  }

  /**
   * 相似度校准
   */
  private var shrinkDownSimilarityLambda: Double = 50.0

  def setShrinkDownSimilarityLambda(shrinkDownSimilarityLambda: Double): UserCF = {
    this.shrinkDownSimilarityLambda = shrinkDownSimilarityLambda
    this
  }

  def getShrinkDownSimilarityLambda(): Double = {
    this.shrinkDownSimilarityLambda
  }

  /**
   * 用户相似矩阵
   */
  private var userSimilarityDF: DataFrame = _

  private def setUserSimilarityDF(userSimilarityDF: DataFrame): UserCF = {
    this.userSimilarityDF = userSimilarityDF
    this
  }

  def fit(rawDataDF: DataFrame): UserCF = {
    val spark = rawDataDF.sparkSession
    this.spark = spark
    import spark.implicits._

    val dataDF = rawDataDF
      .groupBy(userColumnName, itemColumnName)
      .agg(max(ratingColumnName).as(ratingColumnName))
      .persist(StorageLevel.MEMORY_AND_DISK)
    this.dataDF = dataDF

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

    // 通过item倒排索引计算user pair
    val coCccurrenceUserPairRDD: RDD[((String, String), (Double, Double, Double, Int))] = dataDF
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
          val prefix = Random.nextInt(100) + "-"
          for (j <- (i + 1).until(itemSet.size)) {
            buffer.+=(
              (
                (prefix + itemSet(i)._1, itemSet(j)._1)
                , (itemSet(i)._2 * itemSet(j)._2, math.pow(itemSet(i)._2, 2.0), math.pow(itemSet(j)._2, 2.0), 1))
            )
          }
        }
        buffer
      })

    // 计算用户相似度
    val userSimilarityDF = coCccurrenceUserPairRDD
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3, x._4 + y._4))
      .filter(_._2._4 >= minCommonItemNum)
      .map(x => ((x._1._1.split("-")(1), x._1._2), x._2))
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

    setUserSimilarityDF(userSimilarityDF)
    println(s"[${this.getClass.getSimpleName}.fit] this.userSimilarityDF.size:${this.userSimilarityDF.count()}")
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
