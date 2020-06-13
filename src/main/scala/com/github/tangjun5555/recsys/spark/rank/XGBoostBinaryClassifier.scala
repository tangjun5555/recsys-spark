package com.github.tangjun5555.recsys.spark.rank

import ml.dmlc.xgboost4j.scala.spark.{XGBoostClassificationModel, XGBoostClassifier}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, udf}

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

  private var params: Map[String, Any] = Map(
    "objective" -> "binary:logistic"  // 目标函数
  )

  private var xgboostModel: XGBoostClassificationModel = null

  /**
   * 训练
   * @param rawDataDF
   * @return
   */
  def fit(rawDataDF: DataFrame): this.type = {
    this.xgboostModel = new XGBoostClassifier(params)
      .setFeaturesCol(featuresColumnName)
      .setLabelCol(labelColumnName)
      .fit(rawDataDF)

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

  /**
   * 加载之前训练好的模型
   * TODO
   * @param modelFilePath
   * @return
   */
  def load(modelFilePath: String): this.type = {
    this
  }

  /**
   * 保存模型
   * 只能保存为二进制格式
   * @param modelFilePath
   * @return
   */
  def save(modelFilePath: String): this.type = {
    this.xgboostModel.save(modelFilePath)
    this
  }

}
