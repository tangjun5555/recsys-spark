package org.apache.spark.ml.fm

import com.github.tangjun5555.recsys.spark.jutil.MathFunctionUtil
import org.apache.spark.mllib.linalg.Vector

/**
 * author: tangj
 * time: 2020/4/22 16:35
 * description: FM模型实例类
 */
class FMCustomModel(
                     val weights: Vector // 模型参数
                     , val factorDim: Int // 隐向量维度
                   ) extends Serializable {

  /**
   * 预测
   *
   * @param features
   * @return
   */
  def predict(features: Vector): Double = {
    FMCustomModel.predictAndSum(features, weights, factorDim)._1
  }

}

object FMCustomModel {

  /**
   * @param features
   * @param weights
   * @param factorDim
   * @return
   */
  def predictAndSum(features: Vector, weights: Vector, factorDim: Int): (Double, Array[Double]) = {
    val numFeatures = (weights.size - 1) / (factorDim + 1)

    var logit: Double = weights(0)

    features.foreachActive((i, v) => {
      logit += weights(1 + i) * v
    })

    // 每一维隐向量得分
    val factorSum = Array.fill(factorDim)(0.0)
    for (j <- 0.until(factorDim)) {
      var sumSqr = 0.0
      features.foreachActive((i, v) => {
        val pos = (1 + numFeatures) + i * factorDim + j
        val d = weights(pos) * v
        factorSum(j) += d
        sumSqr += d * d
      })
      logit += 0.5 * (factorSum(j) * factorSum(j) - sumSqr)
    }

    val probability = MathFunctionUtil.sigmoid(logit)
    (probability, factorSum)
  }

}
