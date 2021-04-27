package org.apache.spark.ml.evaluation

import org.apache.spark.sql.Dataset

/**
 * author: tangj 1844250138@qq.com
 * time: 2021/4/26 11:20 上午
 * description: 预估准度PCOPC(Predict Click Over Post Click)
 */
class PCOPCEvaluator extends BinaryClassifierEvaluator {

  override def evaluate(dataset: Dataset[_]): Double = {
    dataset.toDF().rdd.map(_.getAs[Double](predictionCol)).sum() / dataset.toDF().rdd.map(_.getAs[Double](labelCol)).sum()
  }

}
