package indi.tangjun.recsys.spark.`match`

import org.apache.spark.ml.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
 * author: tangjun 1844250138@qq.com
 * time: 2020/2/27 11:48
 * description:
 */
class Item2Vec extends ItemEmbedding {

  private var spark: SparkSession = _

  /**
   * 保留训练数据
   */
  private var dataDF: DataFrame = _

  private var itemColumnName: String = "item"

  def setItemColumnName(value: String): this.type = {
    this.itemColumnName = value
    this
  }

  def getItemColumnName(): String = {
    this.itemColumnName
  }

  private var vectorColumnName = "vector"

  def setVectorColumnName(value: String): this.type = {
    this.vectorColumnName = value
    this
  }

  def getVectorColumnName(): String = {
    this.vectorColumnName
  }

  /**
   * 序列样本id
   */
  private var sampleIdColumnName: String = "sample_id"

  def setSampleIdColumnName(value: String): this.type = {
    this.sampleIdColumnName = value
    this
  }

  def getSampleIdColumnName(): String = {
    this.sampleIdColumnName
  }

  private var sequenceColumnName: String = "sequence"

  def setSequenceColumnName(value: String): this.type = {
    this.sequenceColumnName = value
    this
  }

  def getSequenceColumnName(): String = {
    this.sequenceColumnName
  }

  private var vectorSize: Int = 32

  def setVectorSize(value: Int): this.type = {
    this.vectorSize = value
    this
  }

  def getVectorSize(): Int = {
    this.vectorSize
  }

  /**
   * 最大迭代次数
   */
  private var iter: Int = 20

  def setIter(value: Int): this.type = {
    this.iter = value
    this
  }

  def getIter(): Int = {
    this.iter
  }

  private var learningRate: Double = 0.2

  def setLearningRate(learningRate: Double): Item2Vec = {
    this.learningRate = learningRate
    this
  }

  def getLearningRate(): Double = {
    this.learningRate
  }

  private var word2Vec: Word2VecModel = _

  private var itemVectorDF: DataFrame = _

  def fit(rawDataDF: DataFrame): Item2Vec = {
    val spark = rawDataDF.sparkSession
    this.spark = spark

    val dataDF = rawDataDF
      .select(sampleIdColumnName, sequenceColumnName)
      .persist(StorageLevel.MEMORY_AND_DISK)
    this.dataDF = dataDF

    val sequenceMaxLength: Int = dataDF.select(sequenceColumnName)
      .rdd.map(row => row.getAs[Seq[String]](sequenceColumnName).size)
      .max()

    this.word2Vec = new Word2Vec()
      .setInputCol(sequenceColumnName)
      .setOutputCol(vectorColumnName)
      .setMaxIter(iter)
      .setVectorSize(vectorSize)
      .setStepSize(learningRate) // Learning rate
      .setMaxSentenceLength(sequenceMaxLength)
      .setWindowSize(sequenceMaxLength)
      .setNumPartitions(spark.conf.get("spark.default.parallelism").toInt / 2)
      .setMinCount(0)
      .setSeed(555L)
      .fit(rawDataDF)
    // "word", "vector"
    this.itemVectorDF = this.word2Vec.getVectors
    this
  }

  override def getItemEmbedding(vectorAsString: Boolean = false): DataFrame = {
    val spark = this.spark
    import spark.implicits._
    if (vectorAsString) {
      this.itemVectorDF
        .rdd.map(row => (row.getAs[String]("word"), row.getAs[Seq[Float]]("vector").mkString(",")))
        .toDF(itemColumnName, vectorColumnName)
    } else {
      this.itemVectorDF.withColumnRenamed("word", itemColumnName)
        .withColumnRenamed("vector", itemColumnName)
    }
  }

}
