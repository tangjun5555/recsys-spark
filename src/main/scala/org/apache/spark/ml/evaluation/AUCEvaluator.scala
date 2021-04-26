package org.apache.spark.ml.evaluation

import org.apache.spark.sql.Dataset

/**
 * author: tangj 1844250138@qq.com
 * time: 2020/8/31 12:11 下午
 * description:
 */
class AUCEvaluator extends BinaryClassifierEvaluator {

  override def evaluate(dataset: Dataset[_]): Double = {
    new BinaryClassificationEvaluator()
      .setLabelCol(labelColumnName)
      .setRawPredictionCol(predictionColumnName)
      .setMetricName("areaUnderROC")
      .evaluate(dataset)
  }

}
