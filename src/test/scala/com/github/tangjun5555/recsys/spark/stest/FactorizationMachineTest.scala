package com.github.tangjun5555.recsys.spark.stest

import com.github.tangjun5555.recsys.spark.rank.FactorizationMachine
import com.github.tangjun5555.recsys.spark.util.SparkUtil
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.storage.StorageLevel

/**
 * author: tangj
 * time: 2020/4/24 10:49
 * description:
 */
object FactorizationMachineTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkUtil.getSparkSession(this.getClass.getSimpleName)
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

    val model = new FactorizationMachine()
      .setEpoch(20)
      .setMiniBatchFraction(0.2)
      .setFactorDim(4)
      .setRegularizationType("L1")
      .setLearningRate(0.01)
      .setOptimizer("LBFGS")
      .fit(trainDF)

    val trainPreDF = model.predict(trainDF)
    val validPreDF = model.predict(validDF)

//    val evaluator = new AUCAndLogLossEvaluator()
//    val (trainLogLoss, trainAUC) = evaluator.evaluate(trainPreDF)
//    val (validLogLoss, validAUC) = evaluator.evaluate(validPreDF)
//
//    println(s"trainLogLoss:${trainLogLoss}, trainAUC:${trainAUC}")
//    println(s"validLogLoss:${validLogLoss}, validAUC:${validAUC}")

    Thread.sleep(1000 * 1000)
    spark.stop()
  }

}
