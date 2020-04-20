package indi.tangjun.recsys.spark.rank

import indi.tangjun.recsys.spark.jutil.MathFunctionUtil
import org.apache.spark.ml.linalg._
import org.apache.spark.rdd.RDD

import scala.util.Random

/**
 * author: tangjun
 * time: 2020/4/20 17:30
 * description:
 */
class FactorizationMachine {

}


/**
 * Factorization Machine model.
 */
class FMModel(
               val factorMatrix: Matrix,
               val weightVector: Option[Vector],
               val intercept: Double
             )
  extends Serializable {

  val numFeatures = factorMatrix.numCols
  val numFactors = factorMatrix.numRows
  require(numFeatures > 0 && numFactors > 0)

  def predict(testData: Vector): Double = {
    require(testData.size == numFeatures)
    var pred = intercept
    if (weightVector.isDefined) {
      testData.foreachActive {
        case (i, v) =>
          pred += weightVector.get(i) * v
      }
    }
    for (f <- 0 until numFactors) {
      var sum = 0.0
      var sumSqr = 0.0
      testData.foreachActive {
        case (i, v) =>
          val d = factorMatrix(f, i) * v
          sum += d
          sumSqr += d * d
      }
      pred += (sum * sum - sumSqr) * 0.5
    }
    MathFunctionUtil.sigmoid(pred)
  }

  def predict(testData: RDD[Vector]): RDD[Double] = {
    testData.mapPartitions {
      _.map {
        vec =>
          predict(vec)
      }
    }
  }

  protected def formatVersion: String = "1.0"
}


class FMWithSGD(
                 private var stepSize: Double,
                 private var numIterations: Int,
                 private var dim: (Boolean, Boolean, Int),
                 private var regParam: (Double, Double, Double)
               ) extends Serializable {

  /**
   * Construct an object with default parameters:
   * {
   * stepSize: 1.0,
   * numIterations: 100,
   * dim: (true, true, 8),
   * regParam: (0, 0.01, 0.01)
   * }.
   */
  def this() = this(1.0, 100, (true, true, 8), (0, 1e-3, 1e-4))

  private var k0: Boolean = dim._1
  private var k1: Boolean = dim._2
  private var k2: Int = dim._3

  private var r0: Double = regParam._1
  private var r1: Double = regParam._2
  private var r2: Double = regParam._3

  private var initMean: Double = 0
  private var initStd: Double = 0.01

  private var numFeatures: Int = -1

  /**
   * A (Boolean,Boolean,Int) 3-Tuple stands for whether the global bias term should be used, whether the one-way
   * interactions should be used, and the number of factors that are used for pairwise interactions, respectively.
   */
  def setDim(dim: (Boolean, Boolean, Int)): this.type = {
    require(dim._3 > 0)
    this.k0 = dim._1
    this.k1 = dim._2
    this.k2 = dim._3
    this
  }

  /**
   * A (Double,Double,Double) 3-Tuple stands for the regularization parameters of intercept, one-way
   * interactions and pairwise interactions, respectively.
   */
  def setRegParam(regParams: (Double, Double, Double)): this.type = {
    require(regParams._1 >= 0 && regParams._2 >= 0 && regParams._3 >= 0)
    this.r0 = regParams._1
    this.r1 = regParams._2
    this.r2 = regParams._3
    this
  }

  /**
   * initStd Standard Deviation used for factorization matrix initialization.
   */
  def setInitStd(initStd: Double): this.type = {
    require(initStd > 0)
    this.initStd = initStd
    this
  }

  /**
   * Set the number of iterations for SGD.
   */
  def setNumIterations(numIterations: Int): this.type = {
    require(numIterations > 0)
    this.numIterations = numIterations
    this
  }

  /**
   * Set the initial step size of SGD for the first step.
   * In subsequent steps, the step size will decrease with stepSize/sqrt(t)
   */
  def setStepSize(stepSize: Double): this.type = {
    require(stepSize >= 0)
    this.stepSize = stepSize
    this
  }

  /**
   * Encode the FMModel to a dense vector, with its first numFeatures * numFactors elements representing the
   * factorization matrix v, sequential numFeaturs elements representing the one-way interactions weights w if k1 is
   * set to true, and the last element representing the intercept w0 if k0 is set to true.
   * The factorization matrix v is initialized by Gaussinan(0, initStd).
   * v : numFeatures * numFactors + w : [numFeatures] + w0 : [1]
   */
  private def generateInitWeights(): Vector = {
    (k0, k1) match {
      case (true, true) =>
        Vectors.dense(Array.fill(numFeatures * k2)(Random.nextGaussian() * initStd + initMean)
          ++ Array.fill(numFeatures + 1)(0.0))

      case (true, false) =>
        Vectors.dense(Array.fill(numFeatures * k2)(Random.nextGaussian() * initStd + initMean)
          ++ Array(0.0))

      case (false, true) =>
        Vectors.dense(Array.fill(numFeatures * k2)(Random.nextGaussian() * initStd + initMean)
          ++ Array.fill(numFeatures)(0.0))

      case (false, false) =>
        Vectors.dense(Array.fill(numFeatures * k2)(Random.nextGaussian() * initStd + initMean))
    }
  }

  /**
   * Create a FMModle from an encoded vector.
   */
  private def createModel(weights: Vector): FMModel = {
    val values = weights.toArray
    val v = new DenseMatrix(k2, numFeatures, values.slice(0, numFeatures * k2))
    val w = if (k1) Some(Vectors.dense(values.slice(numFeatures * k2, numFeatures * k2 + numFeatures))) else None
    val w0 = if (k0) values.last else 0.0
    new FMModel(v, w, w0)
  }



}

