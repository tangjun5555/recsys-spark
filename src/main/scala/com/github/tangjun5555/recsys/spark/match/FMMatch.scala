package com.github.tangjun5555.recsys.spark.`match`

import com.github.tangjun5555.recsys.spark.rank.FactorizationMachine
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * author: tangj
 * time: 2020/5/16 18:37
 * description:
 */
class FMMatch extends UserEmbedding with ItemEmbedding {

  private var spark: SparkSession = null

  private var labelColumnName: String = "label"

  def setLabelColumnName(value: String): this.type = {
    this.labelColumnName = value
    this
  }

  private var allFeaturesColumnName: String = "features"

  def setFeaturesColumnName(value: String): this.type = {
    this.allFeaturesColumnName = value
    this
  }

  private var userFeaturesColumnName: String = "userFeatures"

  def setUserFeaturesColumnName(value: String): this.type = {
    this.userFeaturesColumnName = value
    this
  }

  private var itemFeaturesColumnName: String = "itemFeatures"

  def setItemFeaturesColumnName(value: String): this.type = {
    this.itemFeaturesColumnName = value
    this
  }

  private var sampleWeightColumnName: String = "sample_weight"

  def setSampleWeightColumnName(value: String): this.type = {
    this.sampleWeightColumnName = value
    this
  }

  private var fm: FactorizationMachine = null

  /**
   * @param rawDataDF
   * @return
   */
  def fit(rawDataDF: DataFrame): this.type = {

    this
  }

  override def getUserEmbedding(vectorAsString: Boolean): DataFrame = ???

  override def getItemEmbedding(vectorAsString: Boolean): DataFrame = ???

}
