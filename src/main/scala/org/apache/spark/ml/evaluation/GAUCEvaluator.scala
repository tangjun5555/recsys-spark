package org.apache.spark.ml.evaluation

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
 * author: tangj 1844250138@qq.com
 * time: 2020/5/3 10:56
 * description: Group AUC
 * Reference: Deep Interest Network for Click-Through Rate Prediction
 * group一般可以设置为user_id、session_id
 *
 */
class GAUCEvaluator extends BinaryClassifierEvaluator {

  private var groupCol: String = "group_id"

  def setGroupCol(value: String): this.type = {
    this.groupCol = value
    this
  }

  def evaluate(dataset: Dataset[_]): Double = {
    val spark: SparkSession = dataset.sparkSession
    import spark.implicits._

    val dataDF: DataFrame = dataset.toDF().rdd
      .map(row =>
        (
          row.getAs[String](groupCol)
          , row.getAs[Double](predictionCol)
          , row.getAs[Double](labelCol)
        )
      )
      .toDF(groupCol, predictionCol, labelCol)
      .persist(StorageLevel.MEMORY_AND_DISK)
    val dataTableName = s"GAUCEvaluator_data_${System.currentTimeMillis()}"
    dataDF.createOrReplaceTempView(dataTableName)

    val groupWeightDF: DataFrame = spark.sql(
      s"""
         |select ${groupCol}
         |  , cast(count(${labelCol}) as double) group_weight
         |from ${dataTableName}
         |group by ${groupCol}
         |"""
        .stripMargin
    )

    val result: (Double, Double) = dataDF.join(groupWeightDF, Seq(groupCol), "inner")
      .rdd
      .map(row => {
        val groupId = row.getAs[String](groupCol)
        val groupWeight = row.getAs[Double]("group_weight")
        val prediction = row.getAs[Double](predictionCol)
        val label = row.getAs[Double](labelCol)
        ((groupId, groupWeight), (prediction, label, 1.0))
      })
      .groupByKey()
      .map(row => {
        if (row._2.toSeq.map(_._2).distinct.size != 2) {
          (0.0, 0.0)
        } else {
          val pairs: Seq[(Double, Double, Double)] = row._2.toSeq
            .sortBy(x => (x._1, 1.0 - x._2))
            .reverse
          val positiveNum = pairs.filter(_._2 == 1.0).map(_._3).sum
          val negativeNum = pairs.filter(_._2 == 0.0).map(_._3).sum
          var count = 0.0
          for (i <- 0.until(pairs.size - 1) if pairs(i)._2 == 1.0) {
            for (j <- (i + 1).until(pairs.size) if pairs(j)._2 == 0.0) {
              if (pairs(i)._1 > pairs(j)._1) {
                count += 1.0
              } else if (pairs(i)._1.equals(pairs(j)._1)) {
                count += 0.5
              }
            }
          }
          val groupAuc = count / (positiveNum * negativeNum)
          (row._1._2, row._1._2 * groupAuc)
        }
      })
      .reduce((x, y) => (x._1 + y._1, x._2 + y._2))
    result._2 / result._1
  }

}
