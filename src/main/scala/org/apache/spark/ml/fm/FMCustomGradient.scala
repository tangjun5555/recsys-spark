package org.apache.spark.ml.fm

import indi.tangjun.recsys.spark.jutil.MathFunctionUtil
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.optimization.Gradient

/**
 * author: tangj
 * time: 2020/4/22 16:30
 * description: FM梯度实例类
 */
class FMCustomGradient(val factorDim: Int) extends Gradient {

  //  def weightedCompute(data: linalg.Vector, label: Double, weights: linalg.Vector, sampleWeight: Double): (linalg.Vector, Double) = {
  //    val gradient = Vectors.zeros(weights.size)
  //    val loss = weightedCompute(data, label, weights, sampleWeight, gradient)
  //    (gradient, loss)
  //  }
  //
  //  def weightedCompute(data: linalg.Vector, label: Double, weights: linalg.Vector, sampleWeight: Double, cumGradient: linalg.Vector): Double = {
  //    val (probability, factorSum) = FMCustomModel.predictAndSum(data, weights, factorDim)
  //
  //    0.0
  //  }
  //
  //  override def compute(data: linalg.Vector, label: Double, weights: linalg.Vector): (linalg.Vector, Double) = {
  //    weightedCompute(data, label, weights, 1.0)
  //  }
  //
  //  override def compute(data: linalg.Vector, label: Double, weights: linalg.Vector, cumGradient: linalg.Vector): Double = {
  //    weightedCompute(data, label, weights, 1.0, cumGradient)
  //  }

  override def compute(
                        data: linalg.Vector
                        , label: Double
                        , weights: linalg.Vector
                        , cumGradient: linalg.Vector
                      ): Double = {
    val numFeatures = (weights.size - 1) / (factorDim + 1)

    val (probability, factorSum) = FMCustomModel.predictAndSum(data, weights, factorDim)
    val loss = MathFunctionUtil.binaryLogLoss(label, probability)

    val cumGradientValues = cumGradient.toDense.values
    val dLdZ = probability - label

    cumGradientValues(0) += dLdZ

    data.foreachActive {
      case (i, v) =>
        cumGradientValues(1 + i) += v * dLdZ
    }

    for (j <- 0.until(factorDim)) {
      data.foreachActive((i, v) => {
        val pos = (1 + numFeatures) + i * factorDim + j
        cumGradientValues(pos) += (factorSum(j) * v - weights(pos) * v * v) * dLdZ
      })
    }

    loss
  }

}
