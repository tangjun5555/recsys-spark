package com.github.tangjun5555.recsys.spark.stest

import com.github.tangjun5555.recsys.spark.util.SparkUtil
import org.apache.spark.ml.evaluation.CPMSensitiveAUCEvaluator
import org.apache.spark.sql.DataFrame

/**
 * author: tangj 1844250138@qq.com
 * time: 2021/1/12 2:32 下午
 * description:
 */
object CPMSensitiveAUCEvaluatorTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkUtil.getSparkSession(this.getClass.getSimpleName)
    import spark.implicits._

    val data: DataFrame = spark.sparkContext.makeRDD(Seq(
      ("D", 0.9, 2.0, 1.0)
      , ("C", 0.8, 3.0, 1.0)
      , ("B", 0.7, 4.0, 1.0)
      , ("A", 0.6, 100.0, 1.0)
      , ("E", 0.5, 999.0, 0.0)
    ))
      .toDF("sample_id", "prediction", "bid", "label")

    data.show(100, false)

    val cpmSensitiveAUCEvaluator = new CPMSensitiveAUCEvaluator()
    println(cpmSensitiveAUCEvaluator.evaluate(data))

    spark.stop()
  }

}
