package com.github.tangjun5555.recsys.spark.`match`

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{count, sum}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer

/**
 * author: tangjun 1844250138@qq.com
 * time: 2020/2/14 14:39
 * description:
 *
 * sim(i,j) =
 *
 */
class Swing extends I2IMatch with U2IMatch {

  private var spark: SparkSession = _

  private def setSpark(spark: SparkSession): Swing = {
    this.spark = spark
    this
  }

  /**
   * 保留训练数据
   */
  private var dataDF: DataFrame = _

  private def setDataDF(dataDF: DataFrame): Swing = {
    this.dataDF = dataDF
    this
  }

  private var userColumnName = "user"

  def setUserColumnName(userColumnName: String): Swing = {
    this.userColumnName = userColumnName
    this
  }

  private var itemColumnName = "item"

  def setItemColumnName(itemColumnName: String): Swing = {
    this.itemColumnName = itemColumnName
    this
  }

  private var similarityColumnName = "sim_score"

  def setSimilarityColumnName(similarityColumnName: String): Swing = {
    this.similarityColumnName = similarityColumnName
    this
  }

  private var alpha: Double = 1.0

  def setAlpha(alpha: Double): Swing = {
    this.alpha = alpha
    this
  }

  /**
   * 控制用户共现矩阵的大小
   */
  private var maxItemRelatedUser: Int = 1000

  def setMaxItemRelatedUser(maxItemRelatedUser: Int): Swing = {
    this.maxItemRelatedUser = maxItemRelatedUser
    this
  }

  /**
   * 每个物品保留前N个相似度最高的物品参与后续计算
   */
  private var maxSimItemNum: Int = 10

  def setMaxSimItemNum(maxSimItemNum: Int) = {
    this.maxSimItemNum = maxSimItemNum
    this
  }

  private var itemSimilarityDF: DataFrame = _

  def getitemSimilarityDF(): DataFrame = {
    this.itemSimilarityDF
  }

  def fit(rawDataDF: DataFrame): Swing = {
    setSpark(rawDataDF.sparkSession)
    val spark = this.spark
    import spark.implicits._

    val dataDF = rawDataDF.select(userColumnName, itemColumnName)
      .distinct()
      .persist(StorageLevel.MEMORY_AND_DISK)
    setDataDF(dataDF)

    println(s"[Swing] dataDF.size:${dataDF.count()}")
    println(s"[Swing] dataDF.user.size:${dataDF.select(userColumnName).distinct().count()}")
    println(s"[Swing] dataDF.item.size:${dataDF.select(itemColumnName).distinct().count()}")

    val userFrequency: Map[String, Long] = dataDF
      .groupBy(userColumnName)
      .agg(count(itemColumnName).as(userColumnName + "_frequency"))
      .rdd.map(row => (row.getAs[String](userColumnName), row.getAs[Long](userColumnName + "_frequency")))
      .collect()
      .toMap
    val userFrequencyBroadcast: Broadcast[Map[String, Long]] = spark.sparkContext.broadcast(userFrequency)

    val itemFrequency: Map[String, Long] = dataDF
      .groupBy(itemColumnName)
      .agg(count(userColumnName).as(itemColumnName + "_frequency"))
      .rdd.map(row => (row.getAs[String](itemColumnName), row.getAs[Long](itemColumnName + "_frequency")))
      .collect()
      .toMap
    val itemFrequencyBroadcast: Broadcast[Map[String, Long]] = spark.sparkContext.broadcast(itemFrequency)

    val userLikeItemsRDD: RDD[(String, String)] = dataDF.rdd
      .map(row => (row.getAs[String](userColumnName), row.getAs[String](itemColumnName)))
      .groupByKey()
      .map(row => (row._1, row._2.mkString(",")))
      .persist(StorageLevel.MEMORY_AND_DISK)
    println(s"[Swing.fit] userLikeItemsRDD:${userLikeItemsRDD.first()}")

    // 格式: ((u1, u2), i1)
    val coCccurrenceUserPairRDD: RDD[((String, String), String)] = dataDF
      .rdd.map(row => (row.getAs[String](itemColumnName), row.getAs[String](userColumnName)))
      .groupByKey()
      .filter(_._2.size >= 2)
      .map(row => {
        if (row._2.size > this.maxItemRelatedUser) {
          println(s"[Swing.fit], item:${row._1}, userSize:${row._2.size}")
          val userFrequencyValue = userFrequencyBroadcast.value
          val userSet = row._2.toSeq.sorted
            .map(x => (x, userFrequencyValue.getOrElse(x, Long.MaxValue)))
            .sortBy(_._2).map(_._1)
            .slice(0, maxItemRelatedUser)
          (row._1, userSet)
        } else {
          (row._1, row._2.toSeq.sorted)
        }
      })
      .flatMap(row => {
        val buffer = ArrayBuffer[((String, String), String)]()
        val userSet = row._2.sorted
        for (i <- 0.until(userSet.size - 1)) {
          for (j <- (i + 1).until(userSet.size)) {
            buffer.+=(((userSet(i), userSet(j)), row._1))
          }
        }
        buffer
      })
      .persist(StorageLevel.MEMORY_AND_DISK)

    val userPairIntersectRDD = coCccurrenceUserPairRDD
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

    // 格式: ((i1, i2), (u1, u2))
    val i2iPairRDD = coCccurrenceUserPairRDD
      .groupByKey()
      .filter(_._2.size >= 2)
      .map(row => {
        if (row._2.size > this.maxItemRelatedUser) {
          println(s"[Swing.fit], userPair:${row._1}, itemSize:${row._2.size}")
          val itemFrequencyValue = itemFrequencyBroadcast.value
          val itemSet = row._2.toSeq.sorted
            .map(x => (x, itemFrequencyValue.getOrElse(x, Long.MaxValue)))
            .sortBy(_._2).map(_._1)
            .slice(0, maxItemRelatedUser)
          (row._1, itemSet)
        } else {
          (row._1, row._2.toSeq.sorted)
        }
      })
      .flatMap(row => {
        val buffer = ArrayBuffer[((String, String), (String, String))]()
        val itemSet = row._2.sorted
        for (i <- 0.until(itemSet.size - 1)) {
          for (j <- (i + 1).until(itemSet.size)) {
            buffer.+=(((itemSet(i), itemSet(j)), row._1))
          }
        }
        buffer
      })

    this.itemSimilarityDF = i2iPairRDD
      .map(row => (row._2, row._1))
      .join(userPairIntersectRDD)
      .map(row => (row._2._1, row._2._2))
      .reduceByKey(_ + _)
      .map(row => (row._1._1, row._1._2, row._2))
      .toDF("i1", "i2", "sim")
      .persist(StorageLevel.MEMORY_AND_DISK)
    println(s"[Swing.fit] this.itemSimilarityDF.size:${this.itemSimilarityDF.count()}")

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
      .map(row => (row.getAs[String]("i1"), row.getAs[String]("i2"), row.getAs[Double]("sim")))
      .flatMap(x => Seq((x._1, (x._2, x._3)), (x._2, (x._1, x._3))))
      .groupByKey()
      .flatMap(x => {
        x._2.toSeq.sortBy(_._2).reverse.slice(0, maxSimItemNum)
          .map(y => (x._1, y._1, y._2))
      })
      .toDF("i1", itemColumnName, "sim")

    dataDF.withColumnRenamed(itemColumnName, "i1")
      .join(finalItemSimilarityDF, Seq("i1"), "inner")
      .drop("i1")
      .withColumnRenamed("i2", itemColumnName)
      .agg(sum("sim").as("final_score"))
      .rdd.map(row => (row.getAs[String](userColumnName), (row.getAs[String](itemColumnName), row.getAs[Double]("final_score"))))
      .groupByKey()
      .map(row => (row._1,
        row._2.toSeq.sortBy(_._2).reverse.slice(0, recNum)
          .map(x => {
            if (withScore) {
              x._1 + ":" + String.format("%.3f", java.lang.Double.valueOf(x._2))
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
        row.getAs[String]("i2"),
        row.getAs[Double]("sim")
      ))
      .flatMap(row => Seq((row._1, (row._2, row._3)), (row._2, (row._1, row._3))))
      .groupByKey()
      .map(row => {
        val recItems = row._2.toSeq.sortBy(_._2).reverse.slice(0, recNum).map(x => {
          if (withScore) {
            x._1 + ":" + String.format("%.3f", java.lang.Double.valueOf(x._2))
          } else {
            x._1
          }
        }).mkString(",")
        (row._1, recItems)
      })
      .toDF(this.itemColumnName, recResultColumnName)
  }

}
