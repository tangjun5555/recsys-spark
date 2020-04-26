package org.apache.spark.ml.fm

import scala.math._
import breeze.linalg.{axpy => brzAxpy, norm => brzNorm, Vector => BV}
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.optimization.{Updater, SimpleUpdater, L1Updater, SquaredL2Updater}

/**
 * author: tangj
 * time: 2020/4/23 10:51
 * description: FM梯度更新实例类
 */
class FMCustomUpdater(val regularizationType: String) extends Updater {

  private val updater = regularizationType match {
    case "L1" => new L1Updater()
    case "L2" => new SquaredL2Updater()
    case _ => new SimpleUpdater()
  }

  override def compute(
                        weightsOld: linalg.Vector
                        , gradient: linalg.Vector // 根据Loss计算出来的梯度，不包括正则项
                        , stepSize: Double // 学习率
                        , iter: Int // 迭代步数
                        , regParam: Double // 正则参数
                      ): (linalg.Vector, Double) = {
    updater.compute(weightsOld, gradient, stepSize, iter, regParam)
  }

}

class SimpleUpdater extends Updater {
  override def compute(
                        weightsOld: Vector,
                        gradient: Vector,
                        stepSize: Double,
                        iter: Int,
                        regParam: Double): (Vector, Double) = {
    val brzWeights: BV[Double] = weightsOld.asBreeze.toDenseVector
    brzAxpy(-stepSize, gradient.asBreeze, brzWeights)
    (Vectors.fromBreeze(brzWeights), 0)
  }
}

class L1Updater extends Updater {
  override def compute(
                        weightsOld: Vector,
                        gradient: Vector,
                        stepSize: Double,
                        iter: Int,
                        regParam: Double): (Vector, Double) = {
    // Take gradient step
    val brzWeights: BV[Double] = weightsOld.asBreeze.toDenseVector
    brzAxpy(-stepSize, gradient.asBreeze, brzWeights)
    // Apply proximal operator (soft thresholding)
    val shrinkageVal = regParam * stepSize
    var i = 0
    val len = brzWeights.length
    while (i < len) {
      val wi = brzWeights(i)
      brzWeights(i) = signum(wi) * max(0.0, abs(wi) - shrinkageVal)
      i += 1
    }
    (Vectors.fromBreeze(brzWeights), brzNorm(brzWeights, 1.0) * regParam)
  }
}

class SquaredL2Updater extends Updater {

  override def compute(
                        weightsOld: Vector,
                        gradient: Vector,
                        stepSize: Double,
                        iter: Int,
                        regParam: Double): (Vector, Double) = {
    // add up both updates from the gradient of the loss (= step) as well as
    // the gradient of the regularizer (= regParam * weightsOld)
    // w' = w - thisIterStepSize * (gradient + regParam * w)
    // w' = (1 - thisIterStepSize * regParam) * w - thisIterStepSize * gradient
    val brzWeights: BV[Double] = weightsOld.asBreeze.toDenseVector
    brzWeights :*= (1.0 - stepSize * regParam)
    brzAxpy(-stepSize, gradient.asBreeze, brzWeights)
    val norm = brzNorm(brzWeights, 2.0)
    (Vectors.fromBreeze(brzWeights), 0.5 * regParam * norm * norm)
  }

}
