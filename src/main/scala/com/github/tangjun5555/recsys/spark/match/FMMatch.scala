package com.github.tangjun5555.recsys.spark.`match`

import com.github.tangjun5555.recsys.spark.rank.FactorizationMachine
import com.github.tangjun5555.recsys.spark.util.VectorUtil
import org.apache.commons.lang3.StringUtils
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.storage.StorageLevel

/**
 * author: tangj
 * time: 2020/5/16 18:37
 * description:
 */
class FMMatch extends UserEmbedding with ItemEmbedding {

  private var spark: SparkSession = _

  private var labelColumnName: String = "label"

  private var sampleWeightColumnName: String = ""

  private var userFeaturesColumnName: String = "userFeatures"

  private var itemFeaturesColumnName: String = "itemFeatures"

  private var vectorColumnName = "vector"

  private var fmFactorDim: Int = 4

  def setLabelColumnName(value: String): this.type = {
    this.labelColumnName = value
    this
  }

  def setSampleWeightColumnName(value: String): this.type = {
    this.sampleWeightColumnName = value
    this
  }

  def setUserFeaturesColumnName(value: String): this.type = {
    this.userFeaturesColumnName = value
    this
  }

  def setItemFeaturesColumnName(value: String): this.type = {
    this.itemFeaturesColumnName = value
    this
  }

  def setVectorColumnName(value: String): this.type = {
    this.vectorColumnName = value
    this
  }

  def setFMFactorDim(value: Int): this.type = {
    this.fmFactorDim = value
    this
  }

  private var userFeatureDim: Int = _

  private var itemFeatureDim: Int = _

  private var fm: FactorizationMachine = null

  /**
   * @param rawDataDF
   * @return
   */
  def fit(rawDataDF: DataFrame): this.type = {
    val spark = rawDataDF.sparkSession
    this.spark = spark
    import spark.implicits._

    val dataDF = rawDataDF.persist(StorageLevel.MEMORY_AND_DISK)
    val allFeatureColumnName = "features"

    userFeatureDim = dataDF.select(userFeaturesColumnName).head(100)
      .map {
        case Row(userFeatures: Vector) => {
          userFeatures.size
        }
      }.head

    itemFeatureDim = dataDF.select(itemFeaturesColumnName).head(100)
      .map {
        case Row(itemFeatures: Vector) => {
          itemFeatures.size
        }
      }.head

    val fmModelData =
      if (!StringUtils.isBlank(sampleWeightColumnName)) {
        dataDF.select(labelColumnName, sampleWeightColumnName, userFeaturesColumnName, itemFeaturesColumnName)
          .rdd.map {
          case Row(label: Double, sampleWeight: Double, userFeatures: Vector, itemFeatures: Vector) => {
            (label, sampleWeight, VectorUtil.concatMLVector(userFeatures, itemFeatures))
          }
        }
          .toDF(labelColumnName, sampleWeightColumnName, allFeatureColumnName)
      } else {
        dataDF.select(labelColumnName, userFeaturesColumnName, itemFeaturesColumnName)
          .rdd.map {
          case Row(label: Double, userFeatures: Vector, itemFeatures: Vector) => {
            (label, VectorUtil.concatMLVector(userFeatures, itemFeatures))
          }
        }
          .toDF(labelColumnName, allFeatureColumnName)
      }

    // TODO
    this.fm = new FactorizationMachine()

      .setLabelColumnName(labelColumnName)
      .setSampleWeightColumnName(sampleWeightColumnName)
      .setFeaturesColumnName(allFeatureColumnName)

      .setEpoch(20)
      .setMiniBatchFraction(0.2)
      .setFactorDim(fmFactorDim)
      .setRegularizationType("L1")
      .setLearningRate(0.01)
      .setOptimizer("GradientDescent")
      .fit(fmModelData)

    this
  }

  /**
   * 待预测用户集
   */
  private var targetUserDF: DataFrame = _

  def setTargetUserDF(value: DataFrame): this.type = {
    this.targetUserDF = value
    this
  }

  override def getUserEmbedding(vectorAsString: Boolean): DataFrame = {
    val buildUserVector = udf(
      (vector: Vector) => {
        var result: Vector = Vectors.zeros(fmFactorDim)
        vector.foreachActive((index: Int, value: Double) => {
          val pos = (1 + userFeatureDim + itemFeatureDim) + index * fmFactorDim
          val tmp = Vectors.dense(this.fm.getFMCustomModel().weights.toArray.slice(pos, pos + fmFactorDim).map(x => x * value))
          result = VectorUtil.add(result, tmp)
        })
      }
    )
    targetUserDF
      .withColumn(vectorColumnName, buildUserVector(col(userFeaturesColumnName)))
  }

  /**
   * 待预测物品集
   */
  private var targetItemDF: DataFrame = _

  def setTargetItemDF(value: DataFrame): this.type = {
    this.targetItemDF = value
    this
  }

  /**
   *
   * @param vectorAsString
   * @return
   */
  override def getItemEmbedding(vectorAsString: Boolean): DataFrame = {
    val buildItemVector = udf(
      (vector: Vector) => {
        var result: Vector = Vectors.zeros(fmFactorDim)
        vector.foreachActive((index: Int, value: Double) => {
          val pos = (1 + userFeatureDim + itemFeatureDim) + (userFeatureDim + index) * fmFactorDim
          val tmp = Vectors.dense(this.fm.getFMCustomModel().weights.toArray.slice(pos, pos + fmFactorDim).map(x => x * value))
          result = VectorUtil.add(result, tmp)
        })
      }
    )
    targetItemDF
      .withColumn(vectorColumnName, buildItemVector(col(itemFeaturesColumnName)))
  }

}
