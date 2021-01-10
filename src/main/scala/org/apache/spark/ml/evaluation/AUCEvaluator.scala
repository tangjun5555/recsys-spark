package org.apache.spark.ml.evaluation

import org.apache.spark.sql.DataFrame

/**
 * author: tangj 1844250138@qq.com
 * time: 2020/8/31 12:11 下午
 * description:
 */
class AUCEvaluator extends Serializable {

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
    val scoreAndLabel = predictions.rdd
      .map(row =>
        (
          row.getAs[Double](predictionColumnName)
          , row.getAs[Double](labelColumnName)
          , 1.0
        )
      )
      .collect()
      // 严格模式，正样本的预测概率要大于负样本
      .sortBy(x => (x._1, 1.0 - x._2))

    if (scoreAndLabel.map(_._2).distinct.length <= 1) {
      0.5
    } else {
      val posNum: Double = scoreAndLabel.filter(_._2 == 1.0).map(_._3).sum
      val negNum: Double = scoreAndLabel.filter(_._2 == 0.0).map(_._3).sum
      var negSum = 0.0
      var posGTNeg = 0.0
      for (p <- scoreAndLabel) {
        if (p._2 == 1.0) {
          posGTNeg += negSum * p._3
        } else {
          negSum += p._3
        }
      }
      posGTNeg / (posNum * negNum)
    }
  }

}
