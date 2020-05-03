package org.apache.spark.ml.evaluation

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
 * author: tangj 1844250138@qq.com
 * time: 2020/5/3 10:56
 * description: Group AUC
 * group一般可以设置为user_id、session_id
 *
 */
class GAUCEvaluator extends Serializable {

  private var groupColumnName: String = "group_id"

  def setGroupColumnName(value: String): this.type = {
    this.groupColumnName = value
    this
  }

  def getGroupColumnName(): String = {
    this.groupColumnName
  }

  private var predictionColumnName: String = "prediction"

  def setPredictionColumnName(value: String): this.type = {
    this.predictionColumnName = value
    this
  }

  def getPredictionColumnName(): String = {
    this.predictionColumnName
  }

  private var labelColumnName: String = "label"

  def setLabelColumnName(value: String): this.type = {
    this.labelColumnName = value
    this
  }

  def getLabelColumnName(): String = {
    this.labelColumnName
  }

  def evaluate(predictions: DataFrame): Double = {
    val dataDF: DataFrame =
      predictions
        .select(groupColumnName, predictionColumnName, labelColumnName)
        .distinct()
        .persist(StorageLevel.MEMORY_AND_DISK)
    dataDF.createTempView("GAUCEvaluator_data")

    val spark: SparkSession = dataDF.sparkSession
    val groupWeightDF: DataFrame = spark.sql(
      s"""
         |select ${groupColumnName}
         |  , count(${labelColumnName}) ${groupColumnName}_weight
         |from GAUCEvaluator_data
         |group ${groupColumnName}
         |having count(distinct ${labelColumnName})=2
         |"""
        .stripMargin
    )

    val result = dataDF.join(groupWeightDF, Seq(groupColumnName), "inner")
      .rdd
      .map(row => {
        val groupId = row.getAs[String](groupColumnName)
        val groupWeight = row.getAs[Int](groupColumnName + "_weight")
        val prediction = row.getAs[Double](predictionColumnName)
        val label = row.getAs[Double](labelColumnName)

        ((groupId, groupWeight), (prediction, label))
      })
      .groupByKey()
      .map(row => {  // 计算每个group的AUC
        val pairs = row._2.toSeq.sortBy(_._1).reverse
        var count = 0
        for (i <- 0.until(pairs.size - 1) if pairs(i)._2 == 1.0) {
          for (j <- (i + 1).until(pairs.size) if pairs(j)._2 == 0.0) {
            // 排除正负例预测值一样的情况
            if (pairs(i)._1 > pairs(j)._1) {
              count += 1
            }
          }
        }
        val positiveNum = pairs.count(x => x._2 == 1.0)
        val negativeNum = pairs.count(x => x._2 == 0.0)
        (row._1._2, count * 1.0 / (positiveNum * negativeNum))
      })
      .reduce((x, y) => (x._1 + y._1, x._1 * x._2 + y._1 * y._2))

    result._2 / result._1
  }

}
