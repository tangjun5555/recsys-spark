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

  protected var labelCol: String = "label"

  def setLabelCol(value: String): this.type = {
    this.labelCol = value
    this
  }

  protected var predictionCol: String = "prediction"

  def setPredictionCol(value: String): this.type = {
    this.predictionCol = value
    this
  }

  def evaluate(dataset: Dataset[_]): Double

  override def copy(extra: ParamMap): Evaluator = {
    this
  }

  override val uid: String = Identifiable.randomUID("BinaryClassifierEvaluator")

}
