package org.apache.spark.ml.optimization

import indi.tangjun.recsys.spark.util.VectorUtil
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.mllib.linalg.Vector

import scala.math._
trait AdaUpdater extends Serializable {
  val epsilon = 1e-8
}
/**
 * :: DeveloperApi ::
 * Adam updater for gradient descent with L2 regularization.
 */
@DeveloperApi
class AdamUpdater extends AdaUpdater {
  private var beta1: Double = 0.9
  private var beta2: Double = 0.999

  def compute(
               weightsOld: Vector,
               gradient: Vector,
               mPre: Vector,
               vPre: Vector,
               beta1PowerOld: Double,
               beta2PowerOld: Double,
               iter: Int,
               regParam: Double): (Vector, Vector, Vector, Double, Double, Double) = {
    val decayRate = 1/sqrt(iter)
    val m = beta1 * VectorUtil.toBreeze(mPre) + (1-beta1) * VectorUtil.toBreeze(gradient)
    val v = beta2 * VectorUtil.toBreeze(vPre) + (1-beta2) * (VectorUtil.toBreeze(gradient) :* VectorUtil.toBreeze(gradient))
    val beta1power = beta1PowerOld * beta1
    val beta2power = beta2PowerOld * beta2
    val mc = m / (1.0 - beta1power)
    val vc = v / (1.0 - beta2power)
    val sqv = vc.map(k => sqrt(k))
    val myWeight = VectorUtil.toBreeze(weightsOld) - decayRate * (mc :/ (sqv + epsilon))
    (VectorUtil.fromBreeze(myWeight), VectorUtil.fromBreeze(m), VectorUtil.fromBreeze(v), beta1power, beta2power, 0)
  }
}

/**
 * :: DeveloperApi ::
 * A simple AdaGrad updater for gradient descent *without* any regularization.
 * Uses a step-size decreasing with the square root of the number of iterations.
 * accum += grad * grad
 * var -= lr * grad * (1 / sqrt(accum))
 */
@DeveloperApi
class AdagradUpdater extends AdaUpdater {

  def compute(
               weightsOld: Vector,
               gradient: Vector,
               accumOld: Vector,
               learningRate: Double,
               iter: Int,
               regParam: Double): (Vector, Vector, Double) = {
    val accum = VectorUtil.toBreeze(accumOld) + (VectorUtil.toBreeze(gradient) :* VectorUtil.toBreeze(gradient))
    val sqrtHistGrad = accum.map(k => sqrt(k + epsilon))
    val Weights = VectorUtil.toBreeze(weightsOld) - learningRate * (VectorUtil.toBreeze(gradient) :/ sqrtHistGrad)
    (VectorUtil.fromBreeze(Weights), VectorUtil.fromBreeze(accum), 0)
  }
}
