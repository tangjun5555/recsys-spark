package com.github.tangjun5555.recsys.spark.`match`

import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
 * author: tangjun 1844250138@qq.com
 * time: 2020/2/16 16:01
 * description:
 */
class FunkSVD extends UserEmbedding with ItemEmbedding {

  private var spark: SparkSession = _

  private def setSpark(spark: SparkSession): FunkSVD = {
    this.spark = spark
    this
  }

  /**
   * 保留训练数据
   */
  private var dataDF: DataFrame = _

  private def setDataDF(dataDF: DataFrame): FunkSVD = {
    this.dataDF = dataDF
    this
  }

  private var userColumnName = "user"

  def setUserColumnName(userColumnName: String): FunkSVD = {
    this.userColumnName = userColumnName
    this
  }

  private var itemColumnName = "item"

  def setItemColumnName(itemColumnName: String): FunkSVD = {
    this.itemColumnName = itemColumnName
    this
  }

  private var ratingColumnName = "rating"

  def setRatingColumnName(ratingColumnName: String): FunkSVD = {
    this.ratingColumnName = ratingColumnName
    this
  }

  private var vectorColumnName = "vector"

  def setVectorColumnName(vectorColumnName: String): FunkSVD = {
    this.vectorColumnName = vectorColumnName
    this
  }

  private var recResultColumnName: String = "rec_items"

  def setRecResultColumnName(recResultColumnName: String) = {
    this.recResultColumnName = recResultColumnName
    this
  }

  private var userFactorsDF: DataFrame = _

  private def getUserFactors(): DataFrame = {
    userFactorsDF
  }

  private var itemFactorsDF: DataFrame = _

  private def getItemFactors(): DataFrame = {
    itemFactorsDF
  }

  private var alsModel: ALSModel = _

  private var userDictDF: DataFrame = _

  private var itemDictDF: DataFrame = _

  def fit(rawDataDF: DataFrame
          , maxIter: Int = 5
          , lantentVectorSize: Int = 5
          , regularization: Double = 0.01
          , alpha: Double = 1.0
          , implicitPrefs: Boolean = true
         ): FunkSVD = {

    this.spark = rawDataDF.sparkSession
    val spark = this.spark
    import spark.implicits._

    val dataDF = rawDataDF.select(userColumnName, itemColumnName, ratingColumnName)
      .distinct()
      .persist(StorageLevel.MEMORY_AND_DISK)
    this.dataDF = dataDF
    println(s"[SVDMatch] dataDF.size:${dataDF.count()}")
    println(s"[SVDMatch] dataDF.user.size:${dataDF.select(userColumnName).distinct().count()}")
    println(s"[SVDMatch] dataDF.item.size:${dataDF.select(itemColumnName).distinct().count()}")

    val userDictDF = dataDF.rdd.map(_.getAs[String](userColumnName)).distinct()
      .collect().sorted.zipWithIndex.toSeq
      .toDF(userColumnName, userColumnName + "_index")
      .persist(StorageLevel.MEMORY_AND_DISK)
    this.userDictDF = userDictDF
    val itemDictDF = dataDF.rdd.map(_.getAs[String](itemColumnName)).distinct()
      .collect().sorted.zipWithIndex.toSeq
      .toDF(itemColumnName, itemColumnName + "_index")
      .persist(StorageLevel.MEMORY_AND_DISK)
    this.itemDictDF = itemDictDF
    val modelDataDF = dataDF
      .join(userDictDF, Seq(userColumnName), "inner").drop(userColumnName)
      .join(itemDictDF, Seq(itemColumnName), "inner").drop(itemColumnName)

    val als = new ALS()
      .setUserCol(userColumnName + "_index")
      .setItemCol(itemColumnName + "_index")
      .setRatingCol(ratingColumnName)

      .setMaxIter(maxIter)
      .setRank(lantentVectorSize)

      .setRegParam(regularization)
      .setAlpha(alpha)

      .setColdStartStrategy("drop")
      .setNumUserBlocks(spark.conf.get("spark.default.parallelism").toInt)
      .setNumItemBlocks(spark.conf.get("spark.default.parallelism").toInt)
      .setImplicitPrefs(implicitPrefs)
      .setSeed(555L)
//      .setNonnegative()
    //      .setPredictionCol()
    val alsModel: ALSModel = als.fit(modelDataDF)
    this.alsModel = alsModel

    val userFactorsDF = userDictDF
      .join(alsModel.userFactors.withColumnRenamed("id", userColumnName + "_index"), Seq(userColumnName + "_index"), "inner")
      .select(userColumnName, "features")
      .withColumnRenamed("features", vectorColumnName)
      .persist(StorageLevel.MEMORY_AND_DISK)
    this.userFactorsDF = userFactorsDF
    val itemFactorsDF = itemDictDF
      .join(alsModel.itemFactors.withColumnRenamed("id", itemColumnName + "_index"), Seq(itemColumnName + "_index"), "inner")
      .select(itemColumnName, "features")
      .withColumnRenamed("features", vectorColumnName)
      .persist(StorageLevel.MEMORY_AND_DISK)
    this.itemFactorsDF = itemFactorsDF
    println(s"[SVDMatch] userFactorsDF:${userFactorsDF.head()}")
    println(s"[SVDMatch] itemFactorsDF:${itemFactorsDF.head()}")

    this
  }

  override def getUserEmbedding(vectorAsString: Boolean = false): DataFrame = {
    val spark = this.spark
    import spark.implicits._
    if (vectorAsString) {
      this.userFactorsDF
        .rdd.map(row => (row.getAs[String](userColumnName), row.getAs[Seq[Float]](vectorColumnName).mkString(",")))
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
        .rdd.map(row => (row.getAs[String](itemColumnName), row.getAs[Seq[Float]](vectorColumnName).mkString(",")))
        .toDF(itemColumnName, vectorColumnName)
    } else {
      this.itemFactorsDF
    }
  }

}
