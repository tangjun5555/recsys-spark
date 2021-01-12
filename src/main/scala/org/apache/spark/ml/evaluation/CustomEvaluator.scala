package org.apache.spark.ml.evaluation

import org.apache.spark.sql.DataFrame

/**
 * author: tangj 1844250138@qq.com
 * time: 2021/1/12 11:48 上午
 * description:
 */
trait CustomEvaluator extends Serializable {

  def evaluate(predictions: DataFrame): Double

}
