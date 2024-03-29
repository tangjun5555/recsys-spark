package org.apache.spark.ml.evaluation

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel

/**
 * author: tangj 1844250138@qq.com
 * time: 2020/11/8 5:37 下午
 * description: Mean Average Precision
 */
class MAPEvaluator extends Serializable {

  private var groupColumnName: String = "group_id"

  def setGroupColumnName(value: String): this.type = {
    this.groupColumnName = value
    this
  }

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

  private var k: Int = 10

  def setK(value: Int): this.type = {
    assert(k > 0)
    this.k = value
    this
  }

  def evaluate(predictions: DataFrame): Double = {
    val r1RDD: RDD[(String, Double)] = predictions.rdd.map(row => {
      (row.getAs[String](groupColumnName), (row.getAs[Double](labelColumnName), row.getAs[Double](predictionColumnName)))
    })
      .groupByKey()
      .map(row => {
        val groupId = row._1
        val seq = row._2.toSeq

        val labelCount: Double = seq
          .sortBy(x => x._1)(Ordering(Ordering.Double.reverse))
          .slice(0, k)
          .count(x => x._1 > 0.0)

        if (labelCount == 0.0) {
          (groupId, 0.0)
        } else {
          val tmp: Seq[(Double, Double)] = seq
            .sortBy(x => x._2)(Ordering(Ordering.Double.reverse))
            .slice(0, k)
          val ap = tmp.indices
            .filter(i => tmp(i)._1 > 0.0)
            .map(i => {
              tmp.slice(0, i + 1).count(x => x._1 > 0.0) / (i + 1)
            })
            .sum / labelCount
          (groupId, ap)
        }
      })
      .persist(StorageLevel.MEMORY_AND_DISK)
    val count = r1RDD.count()
    assert(count > 0)
    r1RDD.map(_._2).sum() / count
  }

}
