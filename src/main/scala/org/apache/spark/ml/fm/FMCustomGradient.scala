package org.apache.spark.ml.fm

import com.github.tangjun5555.recsys.spark.jutil.MathFunctionUtil
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.optimization.Gradient

/**
 * author: tangj
 * time: 2020/4/22 16:30
 * description: FM梯度实例类
 */
class FMCustomGradient(val factorDim: Int) extends Gradient {

  def weightedCompute(data: linalg.Vector, label: Double, weights: linalg.Vector, cumGradient: linalg.Vector, sampleWeight: Double): Double = {
    assert(sampleWeight > 0.0)

    val numFeatures = (weights.size - 1) / (factorDim + 1)

    val (probability, factorSum) = FMCustomModel.predictAndSum(data, weights, factorDim)
    val loss = MathFunctionUtil.binaryLogLoss(label, probability)

    val cumGradientValues = cumGradient.toDense.values
    val dLdZ = (probability - label) * sampleWeight

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

  override def compute(
                        data: linalg.Vector
                        , label: Double
                        , weights: linalg.Vector
                        , cumGradient: linalg.Vector
                      ): Double = {
    val realLabel = if (label > 0.0) {
      1.0
    } else {
      0.0
    }
    val realSampleWeight = if (label > 0.0) {
      label
    } else {
      if (label == 0.0) {
        1.0
      } else {
        -label
      }
    }
    weightedCompute(data, realLabel, weights, cumGradient, realSampleWeight)
  }

}
