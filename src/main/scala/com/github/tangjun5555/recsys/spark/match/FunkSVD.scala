package com.github.tangjun5555.recsys.spark.`match`

import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
 * author: tangjun 1844250138@qq.com
 * time: 2020/2/16 16:01
 * description:
 *
 * 显示反馈的损失函数:
 *
 * 隐式反馈的损失函数:
 *
 *
 */
class FunkSVD extends UserEmbedding with ItemEmbedding {

  private var spark: SparkSession = _

  private var dataDF: DataFrame = _

  private var userColumnName = "user"

  private var itemColumnName = "item"

  private var ratingColumnName = "rating"

  private var vectorColumnName = "vector"

  /**
   * 最大迭代次数
   */
  private var maxIter: Int = 20

  /**
   * 向量维度
   */
  private var lantentVectorSize: Int = 16

  /**
   * 正则化参数
   */
  private var regularization: Double = 0.01

  /**
   * 是否是隐式反馈
   */
  private var implicitPrefs: Boolean = true

  /**
   * 隐式反馈的Cui的加权系数
   */
  private var alpha: Double = 1.0

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

  def setVectorColumnName(value: String): this.type = {
    this.vectorColumnName = value
    this
  }

  def setMaxIter(value: Int): this.type = {
    this.maxIter = value
    this
  }

  def setLantentVectorSize(value: Int): this.type = {
    this.lantentVectorSize = value
    this
  }

  def setRegularization(value: Double): this.type = {
    this.regularization = value
    this
  }

  def setImplicitPrefs(value: Boolean): this.type = {
    this.implicitPrefs = value
    this
  }

  def setAlpha(value: Double): this.type = {
    this.alpha = value
    this
  }

  private var userFactorsDF: DataFrame = _

  private var itemFactorsDF: DataFrame = _

  private var alsModel: ALSModel = _

  def fit(rawDataDF: DataFrame): this.type = {
    this.spark = rawDataDF.sparkSession
    val spark = this.spark
    import spark.implicits._

    this.dataDF = rawDataDF.select(userColumnName, itemColumnName, ratingColumnName)
      .distinct()
      .persist(StorageLevel.MEMORY_AND_DISK)
    println(s"${this.getClass.getSimpleName} fit, dataDF.size:${dataDF.count()}")
    println(s"${this.getClass.getSimpleName} fit, dataDF.user.size:${dataDF.select(userColumnName).distinct().count()}")
    println(s"${this.getClass.getSimpleName} fit, dataDF.item.size:${dataDF.select(itemColumnName).distinct().count()}")

    val userDictDF: DataFrame = dataDF.rdd.map(_.getAs[String](userColumnName)).distinct()
      .collect().sorted.zipWithIndex.toSeq
      .toDF(userColumnName, userColumnName + "_index")
      .persist(StorageLevel.MEMORY_AND_DISK)
    val itemDictDF: DataFrame = dataDF.rdd.map(_.getAs[String](itemColumnName)).distinct()
      .collect().sorted.zipWithIndex.toSeq
      .toDF(itemColumnName, itemColumnName + "_index")
      .persist(StorageLevel.MEMORY_AND_DISK)
    val modelDataDF: DataFrame = dataDF
      .join(userDictDF, Seq(userColumnName), "inner").drop(userColumnName)
      .join(itemDictDF, Seq(itemColumnName), "inner").drop(itemColumnName)

    this.alsModel = new ALS()
      .setUserCol(userColumnName + "_index")
      .setItemCol(itemColumnName + "_index")
      .setRatingCol(ratingColumnName)

      .setMaxIter(maxIter)
      .setRank(lantentVectorSize)
      .setRegParam(regularization)

      .setImplicitPrefs(implicitPrefs)
      .setAlpha(alpha)

      .setNumUserBlocks(spark.conf.get("spark.default.parallelism").toInt)
      .setNumItemBlocks(spark.conf.get("spark.default.parallelism").toInt)

      .setSeed(555L)
      .setNonnegative(true)
      .setColdStartStrategy("drop")

      .fit(modelDataDF)

    this.userFactorsDF = userDictDF
      .join(alsModel.userFactors.withColumnRenamed("id", userColumnName + "_index"), Seq(userColumnName + "_index"), "inner")
      .select(userColumnName, "features")
      .withColumnRenamed("features", vectorColumnName)
      .persist(StorageLevel.MEMORY_AND_DISK)
    this.itemFactorsDF = itemDictDF
      .join(alsModel.itemFactors.withColumnRenamed("id", itemColumnName + "_index"), Seq(itemColumnName + "_index"), "inner")
      .select(itemColumnName, "features")
      .withColumnRenamed("features", vectorColumnName)
      .persist(StorageLevel.MEMORY_AND_DISK)
    println(s"${this.getClass.getSimpleName} fit, userFactorsDF:${userFactorsDF.head()}")
    println(s"${this.getClass.getSimpleName} fit, itemFactorsDF:${itemFactorsDF.head()}")

    this
  }

  override def getUserEmbedding(vectorAsString: Boolean = false): DataFrame = {
    val spark = this.spark
    import spark.implicits._
    if (vectorAsString) {
      this.userFactorsDF
        .rdd.map(row => (row.getAs[String](userColumnName), row.getAs[Seq[Float]](vectorColumnName).map(x => String.format("%.7f", java.lang.Double.valueOf(x))).mkString(",")))
        .toDF(userColumnName, vectorColumnName)
    } else {
      this.userFactorsDF
    }
  }

  override def getItemEmbedding(vectorAsString: Boolean = false): DataFrame = {
    val spark = this.spark
    import spark.implicits._
    if (vectorAsString) {
      this.itemFactorsDF
        .rdd.map(row => (row.getAs[String](itemColumnName), row.getAs[Seq[Float]](vectorColumnName).map(x => String.format("%.7f", java.lang.Double.valueOf(x))).mkString(",")))
        .toDF(itemColumnName, vectorColumnName)
    } else {
      this.itemFactorsDF
    }
  }

}
