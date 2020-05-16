package com.github.tangjun5555.recsys.spark.stest

import com.github.tangjun5555.recsys.spark.rank.XGBoostBinaryClassifier
import com.github.tangjun5555.recsys.spark.util.SparkUtil
import org.apache.spark.ml.evaluation.AUCAndLogLossEvaluator
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.storage.StorageLevel

/**
 * author: tangj
 * time: 2020/5/16 15:06
 * description:
 */
object XGBoostBinaryClassifierTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkUtil.getLocalSparkSession(this.getClass.getSimpleName)
    import spark.implicits._

    val trainDF = spark.sparkContext.textFile("data/libsvmtools_datasets/a9a/a9a.tr")
      .map(line => {
        val items = line.trim.split(" ").toSeq
        val label = if (items.head.toInt == 1) {
          1.0
        } else {
          0.0
        }
        val features: Seq[(Int, Double)] = items.tail.map(x => (x.split(":")(0).trim.toInt - 1, x.split(":")(1).trim.toDouble))
        (label, Vectors.sparse(123, features))
      })
      .toDF("label", "features")
      .persist(StorageLevel.MEMORY_AND_DISK)

    val validDF = spark.sparkContext.textFile("data/libsvmtools_datasets/a9a/a9a.te")
      .map(line => {
        val items = line.trim.split(" ").toSeq
        val label = if (items.head.toInt == 1) {
          1.0
        } else {
          0.0
        }
        val features: Seq[(Int, Double)] = items.tail.map(x => (x.split(":")(0).trim.toInt - 1, x.split(":")(1).trim.toDouble))
        (label, Vectors.sparse(123, features))
      })
      .toDF("label", "features")
      .persist(StorageLevel.MEMORY_AND_DISK)

    val model = new XGBoostBinaryClassifier()
      .fit(trainDF)

    val trainPreDF = model.predict(trainDF).persist(StorageLevel.MEMORY_AND_DISK)
    val validPreDF = model.predict(validDF).persist(StorageLevel.MEMORY_AND_DISK)

    trainPreDF.show(10, false)
    validPreDF.show(10, false)

    val evaluator = new AUCAndLogLossEvaluator()
    val (trainLogLoss, trainAUC) = evaluator.evaluate(trainPreDF)
    val (validLogLoss, validAUC) = evaluator.evaluate(validPreDF)

    println(s"trainLogLoss:${trainLogLoss}, trainAUC:${trainAUC}")
    println(s"validLogLoss:${validLogLoss}, validAUC:${validAUC}")

    Thread.sleep(1000 * 1000)
    spark.stop()
  }

}
