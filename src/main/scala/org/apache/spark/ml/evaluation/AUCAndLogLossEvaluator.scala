package org.apache.spark.ml.evaluation

import indi.tangjun.recsys.spark.jutil.MathFunctionUtil
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel

/**
 * author: tangj
 * time: 2020/4/24 13:33
 * description:
 */
class AUCAndLogLossEvaluator extends Serializable {

  private var labelColumnName: String = "label"

  def setLabelColumnName(value: String): this.type = {
    this.labelColumnName = value
    this
  }

  def getLabelColumnName(): String = {
    this.labelColumnName
  }

  private var predictionColumnName: String = "prediction"

  def setPredictionColumnName(value: String): this.type = {
    this.predictionColumnName = value
    this
  }

  def getPredictionColumnName(): String = {
    this.predictionColumnName
  }

  def evaluate(predictions: DataFrame): (Double, Double) = {
    val scoreAndLabel: RDD[(Double, Double)] = predictions.rdd
      .map(row =>
        (row.getAs[Double](predictionColumnName), row.getAs[Double](labelColumnName))
      )
      .persist(StorageLevel.MEMORY_AND_DISK)
    val numSamples = scoreAndLabel.count().toInt

    val logloss: Double = scoreAndLabel.map(x => MathFunctionUtil.binaryLogLoss(x._2, x._1)).sum() / numSamples
    val auc: Double = new BinaryClassificationMetrics(scoreAndLabel).areaUnderROC()

    (logloss, auc)
  }

}
