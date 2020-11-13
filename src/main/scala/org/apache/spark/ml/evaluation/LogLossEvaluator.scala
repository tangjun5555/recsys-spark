package org.apache.spark.ml.evaluation

import com.github.tangjun5555.recsys.spark.jutil.MathFunctionUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel

/**
 * author: tangj 1844250138@qq.com
 * time: 2020/8/31 12:06 下午
 * description:
 */
class LogLossEvaluator extends Serializable {

  private var labelColumnName: String = "label"

  def setLabelColumnName(value: String): this.type = {
    this.labelColumnName = value
    this
  }

  private var predictionColumnName: String = "prediction"

  def setPredictionColumnName(value: String): this.type = {
    this.predictionColumnName = value
    this
  }

  def evaluate(predictions: DataFrame): Double = {
    val scoreAndLabel: RDD[(Double, Double, Double)] = predictions.rdd
      .map(row =>
        (
          row.getAs[Double](predictionColumnName)
          , row.getAs[Double](labelColumnName)
          , 1.0
        )
      )
      .persist(StorageLevel.MEMORY_AND_DISK)
    val numSamples: Double = scoreAndLabel.map(_._3).sum()
    scoreAndLabel.map(x => MathFunctionUtil.binaryLogLoss(x._2, x._1) * x._3).sum() / numSamples
  }

}
