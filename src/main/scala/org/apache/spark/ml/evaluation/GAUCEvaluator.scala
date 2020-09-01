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

  private var sampleWeightColumnName: String = ""

  def setSampleWeightColumnName(value: String): this.type = {
    this.sampleWeightColumnName = value
    this
  }

  def evaluate(predictions: DataFrame): Double = {
    val spark: SparkSession = predictions.sparkSession
    import spark.implicits._

    val dataDF: DataFrame = predictions.rdd
      .map(row =>
        if ("".equals(sampleWeightColumnName) || sampleWeightColumnName == null) {
          (
            row.getAs[String](groupColumnName)
            , row.getAs[Double](predictionColumnName)
            , row.getAs[Double](labelColumnName)
            , 1.0
          )
        } else {
          (
            row.getAs[String](groupColumnName)
            , row.getAs[Double](predictionColumnName)
            , row.getAs[Double](labelColumnName)
            , row.getAs[Double](sampleWeightColumnName)
          )
        }
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
         |having count(distinct ${labelColumnName})>=2
         |"""
        .stripMargin
    )

    val result = dataDF.join(groupWeightDF, Seq(groupColumnName), "inner")
      .rdd
      .map(row => {
        val groupId = row.getAs[String](groupColumnName)
        val groupWeight = row.getAs[Double](groupColumnName + "_weight")
        val prediction = row.getAs[Double](predictionColumnName)
        val label = row.getAs[Double](labelColumnName)
        val sampleWeight = row.getAs[Double]("sample_weight")

        ((groupId, groupWeight), (prediction, label, sampleWeight))
      })
      .groupByKey()
      .map(row => { // 计算每个group的AUC
        val pairs: Seq[(Double, Double, Double)] = row._2.toSeq
          //          .sortBy(_._1)
          .sortBy(x => (x._1, x._2))
          .reverse
        var count = 0.0
        for (i <- 0.until(pairs.size - 1) if pairs(i)._2 == 1.0) {
          for (j <- (i + 1).until(pairs.size) if pairs(j)._2 == 0.0) {
            if (pairs(i)._1 >= pairs(j)._1) {
              count += pairs(i)._3 * pairs(j)._3
            }
          }
        }
        val positiveNum = pairs.filter(_._2 == 1.0).map(_._3).sum
        val negativeNum = pairs.filter(_._2 == 0.0).map(_._3).sum
        val groupAuc = count / (positiveNum * negativeNum)
        (row._1._2, row._1._2 * groupAuc)
      })
      .reduce((x, y) => (x._1 + y._1, x._2 + y._2))

    result._2 / result._1
  }

}
