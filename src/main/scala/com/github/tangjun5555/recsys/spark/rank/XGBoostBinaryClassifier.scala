package com.github.tangjun5555.recsys.spark.rank

import ml.dmlc.xgboost4j.scala.spark.{XGBoostClassificationModel, XGBoostClassifier}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * author: tangj 1844250138@qq.com
 * time: 2020/5/2 10:14
 * description:
 */
class XGBoostBinaryClassifier extends Serializable {

  private var labelColumnName: String = "label"

  def setLabelColumnName(value: String): this.type = {
    this.labelColumnName = value
    this
  }

  private var featuresColumnName: String = "features"

  def setFeaturesColumnName(value: String): this.type = {
    this.featuresColumnName = value
    this
  }

  private var predictionColumnName: String = "prediction"

  def setPredictionColumnName(value: String): this.type = {
    this.predictionColumnName = value
    this
  }

  private var weightColumnName: String = ""

  def setWeightColumnName(value: String): this.type = {
    this.weightColumnName = value
    this
  }

  private var numWorkers: Int = 10

  def setNumWorkers(value: Int): this.type = {
    this.numWorkers = value
    this
  }

  private var numRound: Int = 200

  def setNumRound(value: Int): this.type = {
    this.numRound = value
    this
  }

  private var maxDepth: Int = 6

  def setMaxDepth(value: Int): this.type = {
    this.maxDepth = value
    this
  }

  private var eta: Double = 0.3

  def setEta(value: Double): this.type = {
    this.eta = value
    this
  }

  private var trainTestRatio: Double = 0.1

  def setTrainTestRatio(value: Double): this.type = {
    this.trainTestRatio = value
    this
  }

  private var spark: SparkSession = _

  private var xgboostModel: XGBoostClassificationModel = _

  /**
   * 训练
   *
   * @param rawDataDF
   * @return
   */
  def fit(rawDataDF: DataFrame): this.type = {
    this.spark = rawDataDF.sparkSession

    var model = new XGBoostClassifier()
      .setFeaturesCol(featuresColumnName)
      .setLabelCol(labelColumnName)
      .setPredictionCol(predictionColumnName)

      .setObjective("binary:logistic")
      .setEvalMetric("auc")
      .setTrainTestRatio(trainTestRatio)

      .setNumWorkers(numWorkers)
      .setNumRound(numRound)
      .setMaxDepth(maxDepth)
      .setEta(eta)

      .setMissing(0.0f)
      .setSeed(555L)
      .setSilent(0)

    if (!"".equals(weightColumnName)) {
      model = model.setWeightCol(weightColumnName)
    }

    this.xgboostModel = model.fit(rawDataDF)

    this
  }

  /**
   * 预测
   *
   * @param rawDataDF
   * @return
   */
  def predict(rawDataDF: DataFrame): DataFrame = {
    if (this.xgboostModel != null) {
      val predictCol = udf(
        (features: Vector) => features.toArray(1)
      )
      this.xgboostModel.transform(rawDataDF)
        .drop("prediction")
        .withColumn(predictionColumnName, predictCol(col("probability")))
    } else {
      throw new Exception(s"${this.getClass.getSimpleName} this is not fit before.")
    }
  }

  def getFeatureScore(): DataFrame = {
    val spark = this.spark
    import spark.implicits._

    val scores: Seq[(String, Integer)] = this.xgboostModel.nativeBooster.getFeatureScore().toSeq
    spark.sparkContext.makeRDD(scores).toDF("feature_name", "score")
  }

  def printSummary(): Unit = {
    println(this.xgboostModel.summary.toString())
    println(this.xgboostModel.explainParams())
  }

  /**
   * 加载之前训练好的模型
   *
   * @param modelFilePath
   * @return
   */
  def load(modelFilePath: String): this.type = {
    this.xgboostModel = XGBoostClassificationModel.load(modelFilePath)
    this
  }

  /**
   * 保存模型
   * 只能保存为二进制格式
   *
   * @param modelFilePath
   * @return
   */
  def save(modelFilePath: String): this.type = {
    this.xgboostModel.write.overwrite().save(modelFilePath)
    this
  }

}
