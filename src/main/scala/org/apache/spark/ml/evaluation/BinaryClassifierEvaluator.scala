package org.apache.spark.ml.evaluation

import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.Dataset

/**
 * author: tangj 1844250138@qq.com
 * time: 2021/4/26 2:11 下午
 * description:
 */
trait BinaryClassifierEvaluator extends Evaluator {

  protected var labelColumnName: String = "label"

  def setLabelColumnName(value: String): this.type = {
    this.labelColumnName = value
    this
  }

  protected var predictionColumnName: String = "prediction"

  def setPredictionColumnName(value: String): this.type = {
    this.predictionColumnName = value
    this
  }

  def evaluate(dataset: Dataset[_]): Double

  override def copy(extra: ParamMap): Evaluator = {
    this
  }

  override val uid: String = Identifiable.randomUID("BinaryClassifierEvaluator")

}
