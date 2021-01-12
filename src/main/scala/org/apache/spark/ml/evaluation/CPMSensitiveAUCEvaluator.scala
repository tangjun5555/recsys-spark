package org.apache.spark.ml.evaluation

import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.Dataset

/**
 * author: tangj 1844250138@qq.com
 * time: 2021/1/12 11:36 上午
 * description: csAUC
 * Reference: CPM-sensitive AUC for CTR prediction
 */
class CPMSensitiveAUCEvaluator extends Evaluator {

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

  private var bidColumnName: String = "bid"

  def setBidColumnName(value: String): this.type = {
    this.bidColumnName = value
    this
  }

  override def evaluate(dataset: Dataset[_]): Double = {
    val tuples = dataset.select(labelColumnName, predictionColumnName, bidColumnName)
      .rdd
      .map(row => (
        row.getAs[Double](labelColumnName), row.getAs[Double](predictionColumnName), row.getAs[Double](bidColumnName))
      )
      .collect()
      .toSeq
    if (tuples.length <= 1) {
      0.0
    } else {
      var t1 = 0.0
      var t2 = 0.0
      val posTuples = tuples.filter(x => x._1 == 1.0).sortBy(x => (x._2, x._3))
      val negTuples = tuples.filter(x => x._1 == 0.0)

      for (i <- posTuples.indices) {
        for (j <- negTuples.indices) {
          t2 += posTuples(i)._3
          if (posTuples(i)._2 > negTuples(j)._2) {
            t1 += posTuples(i)._3
          }
        }
      }

      for (i <- 1.until(posTuples.length - 1)) {
        for (j <- (i + 1).until(posTuples.length)) {
          t2 += posTuples(j)._3
          if (posTuples(j)._2 > posTuples(i)._2) {
            t1 += posTuples(j)._3
          } else {
            t1 += posTuples(i)._3
          }
        }
      }

      if (t2 == 0.0) {
        0.0
      } else {
        t1 / t2
      }
    }
  }

  override def copy(extra: ParamMap): Evaluator = {
    this
  }

  override val uid: String = Identifiable.randomUID("CPMSensitiveAUCEvaluator")
}
