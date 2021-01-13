package org.apache.spark.ml.evaluation

import com.github.tangjun5555.recsys.spark.jutil.MathFunctionUtil
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.Dataset
import org.apache.spark.storage.StorageLevel

/**
 * author: tangj 1844250138@qq.com
 * time: 2021/1/13 2:33 下午
 * description:
 */
class F1AtKEvaluator extends Evaluator {

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

  private var k: Int = 20

  def setK(value: Int): this.type = {
    assert(value > 0)
    this.k = value
    this
  }

  private var sep: String = ","

  def setSep(value: String): this.type = {
    this.sep = value
    this
  }

  override def evaluate(dataset: Dataset[_]): Double = {
    val t1 = dataset.select(labelColumnName, predictionColumnName)
      .rdd.map(row => {
      import scala.collection.JavaConverters._
      val labels = row.getAs[String](labelColumnName).split(sep).toSeq.asJava
      val predicts = row.getAs[String](predictionColumnName).split(sep).toSeq.asJava
      val t1 = MathFunctionUtil.computeRecRecallRate(labels, predicts, k)
      val t2 = MathFunctionUtil.computeRecPrecisionRate(labels, predicts, k)
      (2 * t2 * t1) / (t1 + t2)
    })
      .persist(StorageLevel.MEMORY_AND_DISK)
    if (t1.count() == 0) {
      0.0
    } else {
      t1.sum() / t1.count()
    }
  }

  override def copy(extra: ParamMap): Evaluator = {
    this
  }

  override val uid: String = Identifiable.randomUID("PrecisionAtKEvaluator")
}
