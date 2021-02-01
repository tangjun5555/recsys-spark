package org.apache.spark.ml.evaluation

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
 * author: tangj 1844250138@qq.com
 * time: 2020/5/3 10:56
 * description: Group AUC
 * Reference: Deep Interest Network for Click-Through Rate Prediction
 * group一般可以设置为user_id、session_id
 *
 */
class GAUCEvaluator extends Serializable {

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

  private var groupWeightMode: String = "count"

  def setGroupWeightMode(value: String): this.type = {
    assert(Seq("count", "num").contains(value), "value must be count or num")
    this.groupWeightMode = value
    this
  }

  def evaluate(predictions: DataFrame): Double = {
    val spark: SparkSession = predictions.sparkSession
    import spark.implicits._

    val dataDF: DataFrame = predictions.rdd
      .map(row =>
        (
          row.getAs[String](groupColumnName)
          , row.getAs[Double](predictionColumnName)
          , row.getAs[Double](labelColumnName)
          , 1.0
        )
      )
      .toDF(groupColumnName, predictionColumnName, labelColumnName, "sample_weight")
      .persist(StorageLevel.MEMORY_AND_DISK)
    dataDF.createOrReplaceTempView(s"GAUCEvaluator_data")

    // 计算每个group的权重
    val groupWeightDF: DataFrame = spark.sql(
      s"""
         |select ${groupColumnName}
         |  , sum(sample_weight) ${groupColumnName}_weight
         |from GAUCEvaluator_data
         |group by ${groupColumnName}
         |"""
        .stripMargin
    )

    val result: (Double, Double) = dataDF.join(groupWeightDF, Seq(groupColumnName), "inner")
      .rdd
      .map(row => {
        val groupId = row.getAs[String](groupColumnName)
        val groupWeight = if ("count".equals(groupWeightMode)) {
          1.0
        } else {
          row.getAs[Double](groupColumnName + "_weight")
        }
        val prediction = row.getAs[Double](predictionColumnName)
        val label = row.getAs[Double](labelColumnName)
        val sampleWeight = row.getAs[Double]("sample_weight")

        ((groupId, groupWeight), (prediction, label, sampleWeight))
      })
      .groupByKey()
      // 计算每个group的AUC
      .map(row => {
        if (row._2.toSeq.map(_._2).distinct.size != 2) {
          (row._1._2, 0.0)
        } else {
          val pairs: Seq[(Double, Double, Double)] = row._2.toSeq
            .sortBy(x => (x._1, 1.0 - x._2))
            .reverse
          val positiveNum = pairs.filter(_._2 == 1.0).map(_._3).sum
          val negativeNum = pairs.filter(_._2 == 0.0).map(_._3).sum
          var count = 0.0
          // 假设每个组的样本量较少，采用两层循环，便于理解
          for (i <- 0.until(pairs.size - 1) if pairs(i)._2 == 1.0) {
            for (j <- (i + 1).until(pairs.size) if pairs(j)._2 == 0.0) {
              // 严格模式，正样本的预测概率要大于负样本
              if (pairs(i)._1 > pairs(j)._1) {
                count += pairs(i)._3 * pairs(j)._3
              }
            }
          }
          val groupAuc = count / (positiveNum * negativeNum)
          (row._1._2, groupAuc)
        }
      })
      .reduce((x, y) => (x._1 + y._1, x._2 + y._2))
    result._2 / result._1
  }

}
