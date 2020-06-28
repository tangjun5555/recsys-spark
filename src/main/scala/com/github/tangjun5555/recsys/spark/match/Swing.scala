package com.github.tangjun5555.recsys.spark.`match`

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{count, sum}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
 * author: tangjun 1844250138@qq.com
 * time: 2020/2/14 14:39
 * description:
 *
 */
class Swing extends I2IMatch with U2IMatch {

  private var spark: SparkSession = _

  private var dataDF: DataFrame = _

  private var userColumnName = "user"

  private var itemColumnName = "item"

  private var similarityColumnName = "sim_score"

  private var alpha: Double = 5.0

  private var minUserFrequence: Int = 10

  /**
   * 控制用户共现矩阵的大小
   */
  private var maxItemRelatedUser: Int = 1000

  /**
   * 每个物品保留前N个相似度最高的物品参与后续计算
   */
  private var maxSimItemNum: Int = 10

  def setUserColumnName(value: String): this.type = {
    this.userColumnName = value
    this
  }

  def setItemColumnName(value: String): this.type = {
    this.itemColumnName = value
    this
  }

  def setSimilarityColumnName(value: String): this.type = {
    this.similarityColumnName = value
    this
  }

  def setAlpha(value: Double): this.type = {
    this.alpha = value
    this
  }

  def setMinUserFrequence(value: Int): this.type = {
    assert(value >= 2)
    this.minUserFrequence = value
    this
  }

  def setMaxItemRelatedUser(value: Int): this.type = {
    this.maxItemRelatedUser = value
    this
  }

  def setMaxSimItemNum(value: Int): this.type = {
    this.maxSimItemNum = value
    this
  }

  private var itemSimilarityDF: DataFrame = _

  def getitemSimilarityDF(): DataFrame = {
    this.itemSimilarityDF
  }

  def fit(rawDataDF: DataFrame): this.type = {
    this.spark = rawDataDF.sparkSession
    val spark = this.spark
    import spark.implicits._

    this.dataDF = rawDataDF
      .persist(StorageLevel.MEMORY_AND_DISK)
    println(s"${this.getClass.getSimpleName} fit, dataDF.size:${dataDF.count()}")
    println(s"${this.getClass.getSimpleName} fit, dataDF.user.size:${dataDF.select(userColumnName).distinct().count()}")
    println(s"${this.getClass.getSimpleName} fit, dataDF.item.size:${dataDF.select(itemColumnName).distinct().count()}")

    // 计算用户的频率
    val userFrequencyDF: DataFrame = dataDF
      .groupBy(userColumnName)
      .agg(count(itemColumnName).as(userColumnName + "_frequency"))
      .filter(s"${userColumnName + "_frequency"} >= ${minUserFrequence}")
      .persist(StorageLevel.MEMORY_AND_DISK)
    println(s"${this.getClass.getSimpleName} fit, valid user num:${userFrequencyDF.count()}")

    // 计算物品的频率
    val itemFrequencyDF: DataFrame = dataDF
      .groupBy(itemColumnName)
      .agg(count(userColumnName).as(itemColumnName + "_frequency"))
      .persist(StorageLevel.MEMORY_AND_DISK)

    // 基于用户的物品倒排索引表
    val userLikeItemsRDD: RDD[(String, String)] = dataDF.rdd
      .map(row => (row.getAs[String](userColumnName), row.getAs[String](itemColumnName)))
      .groupByKey()
      .map(row => (row._1, row._2.mkString(",")))
      .persist(StorageLevel.MEMORY_AND_DISK)
    println(s"${this.getClass.getSimpleName} fit, userLikeItemsRDD:${userLikeItemsRDD.first()}")

    // 格式: [(u1, u2), (i1,i2)]
    val t1RDD: RDD[((String, String), (String, String))] = dataDF
      .join(itemFrequencyDF, Seq(itemColumnName), "inner")
      .rdd.map(row => (row.getAs[String](userColumnName), (row.getAs[String](itemColumnName), row.getAs[Long](itemColumnName + "_frequency"))))
      .groupByKey()
      .filter(_._2.size >= minUserFrequence)
      .join(userFrequencyDF.rdd.map(row => (row.getAs[String](userColumnName), row.getAs[Long](userColumnName + "_frequency"))))
      .map(x => ((x._1, x._2._2), x._2._1))

      .map(row => {
        if (row._2.size > this.maxItemRelatedUser) {
          println(s"${this.getClass.getSimpleName} fit, user:${row._1}, itemSize:${row._2.size}")
          val itemSet = row._2.toSeq.sortBy(_._1)
            .sortBy(_._2)
            .map(_._1)
            .slice(0, maxItemRelatedUser)
          (row._1, itemSet)
        } else {
          (row._1, row._2.toSeq.sortBy(_._1).map(_._1))
        }
      })
      .flatMap(row => {
        val itemSet = row._2
        for {i <- 0.until(itemSet.size - 1); j <- (i + 1).until(itemSet.size)}
          yield ((itemSet(i), itemSet(j)), row._1)
      })

      .groupByKey()
      .filter(_._2.size >= 2)
      .map(row => {
        if (row._2.size > this.maxItemRelatedUser) {
          println(s"${this.getClass.getSimpleName} fit, item:${row._1}, userSize:${row._2.size}")
          val userSet = row._2.toSeq.sortBy(_._1)
            .sortBy(_._2)
            .map(_._1)
            .slice(0, maxItemRelatedUser)
          (row._1, userSet)
        } else {
          (row._1, row._2.toSeq.sortBy(_._1).map(_._1))
        }
      })
      .flatMap(row => {
        val userSet: Seq[String] = row._2
        for {i <- 0.until(userSet.size - 1); j <- (i + 1).until(userSet.size)}
          yield ((userSet(i), userSet(j)), row._1)
      })
      .persist(StorageLevel.MEMORY_AND_DISK)
    println(s"${this.getClass.getSimpleName} fit, t1RDD.size:${t1RDD.count()}")

    // 用户的交集得分 格式: ((u1, u2), score)
    val userPairIntersectRDD: RDD[((String, String), Double)] = t1RDD
      .map(_._1)
      .distinct()
      .join(userLikeItemsRDD)
      .map(row => (row._2._1, (row._1, row._2._2)))
      .join(userLikeItemsRDD)
      .map(row => ((row._2._1._1, row._1), (row._2._1._2, row._2._2)))
      .map(row => {
        val u1Set = row._2._1.split(",").toSet
        val u2Set = row._2._2.split(",").toSet
        (row._1, 1.0 / (this.alpha + u1Set.intersect(u2Set).size))
      })

    this.itemSimilarityDF = t1RDD
      .join(userPairIntersectRDD)
      .map(row => (row._2._1, row._2._2))
      .reduceByKey(_ + _)
      .map(row => (row._1._1, row._1._2, row._2))
      .toDF("i1", "i2", similarityColumnName)
      .persist(StorageLevel.MEMORY_AND_DISK)

    println(s"${this.getClass.getSimpleName} fit, this.itemSimilarityDF.size:${this.itemSimilarityDF.count()}")

    t1RDD.unpersist()

    this
  }

  override def recommendForUser(
                                 recNum: Int = 50
                                 , withScore: Boolean = false
                                 , recResultColumnName: String = "rec_items"
                               ): DataFrame = {
    val spark = this.spark
    import spark.implicits._

    val finalItemSimilarityDF = this.itemSimilarityDF.rdd
      .map(row => (row.getAs[String]("i1"), row.getAs[String]("i2"), row.getAs[Double](similarityColumnName)))
      .flatMap(x => Seq((x._1, (x._2, x._3)), (x._2, (x._1, x._3))))
      .groupByKey()
      .flatMap(x => {
        x._2.toSeq.sortBy(_._2).reverse.slice(0, maxSimItemNum)
          .map(y => (x._1, y._1, y._2))
      })
      .toDF("i1", itemColumnName, similarityColumnName)

    dataDF.withColumnRenamed(itemColumnName, "i1")
      .join(finalItemSimilarityDF, Seq("i1"), "inner")
      .drop("i1")
      .groupBy(userColumnName, itemColumnName)
      .agg(sum(similarityColumnName).as("final_score"))
      .rdd.map(row => (row.getAs[String](userColumnName), (row.getAs[String](itemColumnName), row.getAs[Double]("final_score"))))
      .groupByKey()
      .map(row => (row._1,
        row._2.toSeq.sortBy(_._2).reverse.slice(0, recNum)
          .map(x => {
            if (withScore) {
              x._1 + ":" + String.format("%.5f", java.lang.Double.valueOf(x._2))
            } else {
              x._1
            }
          })
          .mkString(",")
      ))
      .toDF(userColumnName, recResultColumnName)
  }

  def recommendForItem(
                        recNum: Int = 50
                        , withScore: Boolean = false
                        , recResultColumnName: String = "rec_items"
                      ): DataFrame = {
    val spark = this.spark
    import spark.implicits._

    this.itemSimilarityDF.rdd
      .map(row => (
        row.getAs[String]("i1"),
        row.getAs[String]("i2"),
        row.getAs[Double](similarityColumnName)
      ))
      .flatMap(row => Seq((row._1, (row._2, row._3)), (row._2, (row._1, row._3))))
      .groupByKey()
      .map(row => {
        val recItems = row._2.toSeq.sortBy(_._2).reverse.slice(0, recNum).map(x => {
          if (withScore) {
            x._1 + ":" + String.format("%.5f", java.lang.Double.valueOf(x._2))
          } else {
            x._1
          }
        }).mkString(",")
        (row._1, recItems)
      })
      .toDF(this.itemColumnName, recResultColumnName)
  }

}
