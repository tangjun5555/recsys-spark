package com.github.tangjun5555.recsys.spark.stest

import com.github.tangjun5555.recsys.spark.util.SparkUtil
import org.apache.spark.ml.regression.IsotonicRegression

object IsotonicRegressionTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkUtil.getLocalSparkSession(this.getClass.getSimpleName)

    // Loads data.
    val dataset = spark.read.format("libsvm")
      .load("data/spark/mllib/sample_isotonic_regression_libsvm_data.txt")

    // Trains an isotonic regression model.
    val ir = new IsotonicRegression()
    val model = ir.fit(dataset)

    println(s"Boundaries in increasing order: ${model.boundaries}\n")
    println(s"Predictions associated with the boundaries: ${model.predictions}\n")

    // Makes predictions.
    model.transform(dataset).show()

    spark.stop()
  }

}
