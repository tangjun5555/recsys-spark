package org.apache.spark.ml.fm

import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.optimization.{GradientDescent, LBFGS, Optimizer}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.storage.StorageLevel

import scala.util.Random

/**
 * author: tangj
 * time: 2020/4/23 11:38
 * description: 自定义完整实现FM
 */
class CustomFactorizationMachine extends Serializable {

  private var labelColumnName: String = "label"

  def setLabelColumnName(value: String): this.type = {
    this.labelColumnName = value
    this
  }

  def getLabelColumnName(): String = {
    this.labelColumnName
  }

  private var featuresColumnName: String = "features"

  def setFeaturesColumnName(value: String): this.type = {
    this.featuresColumnName = value
    this
  }

  def getFeaturesColumnName(): String = {
    this.featuresColumnName
  }

  private var predictionColumnName: String = "prediction"

  def setPredictionColumnName(value: String): this.type = {
    this.predictionColumnName = value
    this
  }

  def getPredictionColumnName(): String = {
    this.predictionColumnName
  }

  private var sampleWeightColumnName: String = "sample_weight"

  def setSampleWeightColumnName(value: String): this.type = {
    this.sampleWeightColumnName = value
    this
  }

  def getSampleWeightColumnName(): String = {
    this.sampleWeightColumnName
  }

  private var fmModel: Option[FMCustomModel] = None

  /**
   * 优化方法
   * default GradientDescent
   */
  private var optimMethod: String = "GradientDescent"

  def setOptimizer(value: String): this.type = {
    this.optimMethod = value
    this
  }

  def getOptimizer(): String = {
    this.optimMethod
  }

  /**
   * 隐向量维度
   * default 4
   */
  private var factorDim: Int = 4

  def setFactorDim(value: Int): this.type = {
    this.factorDim = value
    this
  }

  def getFactorDim(): Int = {
    this.factorDim
  }

  /**
   * 迭代轮数
   * default 10
   */
  private var epoch: Int = 10

  def setEpoch(value: Int): this.type = {
    this.epoch = value
    this
  }

  def getEpoch(): Int = {
    this.epoch
  }

  private var batchSize: Option[Int] = None

  def setBatchSize(value: Int): this.type = {
    this.batchSize = Some(value)
    this
  }

  def getBatchSize(): Int = {
    if (this.batchSize.isDefined) {
      this.batchSize.get
    } else {
      0
    }
  }

  //  /**
  //   *
  //   * 每一个epoch的最大迭代轮数
  //   * default 100
  //   * epoch * 1.0 / miniBatchFraction
  //   */
  //  private var maxIter: Int = 100

  /**
   *
   * Fraction of data to be used per iteration.
   * range (0, 1.0]
   */
  private var miniBatchFraction: Double = 0.1

  def setMiniBatchFraction(value: Double): this.type = {
    this.miniBatchFraction = value
    this
  }

  def getMiniBatchFraction(): Double = {
    this.miniBatchFraction
  }

  //  /**
  //   *
  //   * Default is 0.001
  //   * 0.0
  //   */
  //  private var convergenceTol: Double = 0.0
  //
  //  def setConvergenceTol(value: Double): this.type = {
  //    this.convergenceTol = value
  //    this
  //  }

  /**
   *
   * 二阶项初始化的
   * Stdev for initialization of 2-way factors.
   */
  private var initStdev: Double = 0.1

  def setInitStdev(value: Double): this.type = {
    this.initStdev = value
    this
  }

  def getInitStdev(): Double = {
    this.initStdev
  }

  //  /**
  //   *
  //   * Number of partitions to be used for optimization.
  //   */
  //  private var numPartitions: Int = 10

  /**
   *
   * Hyper parameter alpha of learning rate.
   * default 0.2
   */
  private var learningRate: Double = 0.2

  def setLearningRate(value: Double): this.type = {
    this.learningRate = value
    this
  }

  def getLearningRate(): Double = {
    this.learningRate
  }

  /**
   * 正则化类型
   */
  private var regularizationType: String = "L1"

  def setRegularizationType(value: String): this.type = {
    this.regularizationType = value
    this
  }

  def getRegularizationType(): String = {
    this.regularizationType
  }

  /**
   * 正则化参数
   */
  private var regularizationParam: Double = 0.01

  def setRegularizationParam(value: Double): this.type = {
    this.regularizationParam = value
    this
  }

  def getRegularizationParam(): Double = {
    this.regularizationParam
  }

  private var useSampleWeight: Boolean = false

  def setUseSampleWeight(value: Boolean): this.type = {
    this.useSampleWeight = value
    this
  }

  def getUseSampleWeight(): Boolean = {
    this.useSampleWeight
  }

  /**
   * 初始化模型, 用于增量学习
   */
  private var initialModel: Option[FMCustomModel] = None

  def setInitialModel(value: FMCustomModel): this.type = {
    this.initialModel = Some(value)
    this
  }

  /**
   * 初始化参数
   *
   * @param numFeatures
   * @return
   */
  private def initWeights(numFeatures: Int): linalg.Vector = {
      linalg.Vectors.dense(
        Array.fill(1 + numFeatures)(0.0) ++
          Array.fill(numFeatures * factorDim)(new Random(555L).nextGaussian() * initStdev)
      )
  }

  private def extractLabeledPoints(dataset: Dataset[_]): RDD[LabeledPoint] = {
    if (this.useSampleWeight) {
      dataset.select(labelColumnName, sampleWeightColumnName, featuresColumnName)
        .rdd.map {
        case Row(label: Double, sampleWeight: Double, features: Vector) => {
          assert(Seq(0.0, 1.0).contains(label))
          assert(sampleWeight > 0.0)
          if (label == 1.0) {
            LabeledPoint(label * sampleWeight, features)
          } else {
            if (sampleWeight == 1.0) {
              LabeledPoint(label, features)
            } else {
              LabeledPoint(-sampleWeight, features)
            }
          }
        }
      }
    } else {
      dataset.select(labelColumnName, featuresColumnName)
        .rdd.map {
        case Row(label: Double, features: Vector) => {
          assert(Seq(0.0, 1.0).contains(label))
          LabeledPoint(label, features)
        }
      }
    }
  }

  /**
   * @param rawDataDF
   * @return
   */
  def fit(rawDataDF: DataFrame): CustomFactorizationMachine = {
    val labeledPointDataRDD: RDD[LabeledPoint] = extractLabeledPoints(rawDataDF)
    val numFeatures = labeledPointDataRDD.first().features.size
    require(numFeatures > 0)

    val data: RDD[(Double, linalg.Vector)] = labeledPointDataRDD
      .map(lp => (lp.label, linalg.Vectors.fromML(lp.features)))
      .persist(StorageLevel.MEMORY_AND_DISK)
    val numSample: Int = data.count().toInt

    val weights: linalg.Vector = if (initialModel.isDefined) {
      initialModel.get.weights
    } else {
      initWeights(numFeatures)
    }

    // 每一次迭代抽取的样本比例
    val realMiniBatchFraction: Double = if (this.batchSize.isDefined) {
      this.batchSize.get * 1.0 / numSample
    } else {
      this.miniBatchFraction
    }

    // 最大迭代次数
    val maxIter: Int = if (this.batchSize.isDefined) {
      epoch * numSample / this.batchSize.get + 1
    } else {
      (epoch * (1.0 / miniBatchFraction)).toInt + 1
    }

    val gradient = new FMCustomGradient(factorDim)
    val updater: FMCustomUpdater = new FMCustomUpdater(regularizationType)
    val optimizer: Optimizer = optimMethod match {
      case "GradientDescent" =>
        new GradientDescent(gradient, updater)
          .setNumIterations(maxIter)
          .setMiniBatchFraction(realMiniBatchFraction) // 每一次迭代抽取的样本比例

          .setStepSize(learningRate) // 学习率
          .setRegParam(regularizationParam) // 正则化参数

          //          .setConvergenceTol(convergenceTol)
          .setConvergenceTol(0.0)

      case "LBFGS" => new LBFGS(gradient, updater)
        .setNumIterations(maxIter)
        .setRegParam(regularizationParam)
        .setConvergenceTol(0.0)
        .setNumCorrections(10)

      case _ => throw new IllegalArgumentException(s"${this.getClass.getSimpleName} do not support ${optimMethod} now.")
    }
    val newWeights = optimizer.optimize(data, weights)

    val fmModel: FMCustomModel = new FMCustomModel(newWeights, factorDim)
    this.fmModel = Some(fmModel)

    this
  }

  /**
   * 预测
   *
   * @param rawDataDF
   * @return
   */
  def predict(rawDataDF: DataFrame): DataFrame = {
    if (this.fmModel.isDefined) {
      val predictCol = udf(
        (features: Vector) => this.fmModel.get.predict(linalg.Vectors.fromML(features))
      )
      rawDataDF
        .withColumn(predictionColumnName, predictCol(col(featuresColumnName)))
    } else {
      throw new Exception(s"${this.getClass.getSimpleName} this is not fit before.")
    }
  }

}
