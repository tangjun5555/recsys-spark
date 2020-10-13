package com.github.tangjun5555.recsys.spark.`match`

import com.github.tangjun5555.recsys.spark.util.STimeUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
 * author: tangj 1844250138@qq.com
 * time: 2020/10/12 6:00 下午
 * description:
 */
class DeepWalkV2 extends ItemEmbedding {

  private def randomChoice(values: Seq[(String, Double)]): String = {
    var r = Random.nextDouble() * values.map(_._2).sum
    var result = values.head._1
    for (i <- values.indices if r > 0.0) {
      r = r - values(i)._2
      result = values(i)._1
    }
    result
  }

  /**
   * 保留训练数据
   */
  private var dataDF: DataFrame = _

  private var spark: SparkSession = _

  private var userColumnName: String = "user"

  def setUserColumnName(value: String): this.type = {
    this.userColumnName = value
    this
  }

  private var itemColumnName: String = "item"

  def setItemColumnName(value: String): this.type = {
    this.itemColumnName = value
    this
  }

  private var ratingColumnName = "rating"

  def setRatingColumnName(value: String): this.type = {
    this.ratingColumnName = value
    this
  }

  private var timestampColumnName: String = "timestamp"

  def setTimestampColumnName(value: String): this.type = {
    this.timestampColumnName = value
    this
  }

  private var vectorColumnName = "vector"

  def setVectorColumnName(value: String): this.type = {
    this.vectorColumnName = value
    this
  }

  private var walkEpoch: Int = 10

  def setWalkEpoch(value: Int): this.type = {
    this.walkEpoch = value
    this
  }

  private var walkLength: Int = 30

  def setWalkLength(value: Int): this.type = {
    assert(value > 2)
    this.walkLength = value
    this
  }

  private var windowSize: Int = 3

  def setWindowSize(value: Int): this.type = {
    assert(windowSize > 0)
    this.windowSize = value
    this
  }

  private var realRandomWalkPaths: RDD[Seq[String]] = _

  def fit(rawDataDF: DataFrame): this.type = {
    val spark = rawDataDF.sparkSession
    this.spark = spark

    this.dataDF = rawDataDF.persist(StorageLevel.MEMORY_AND_DISK)
    dataDF.show(30, false)

    // 统计基本信息
    println(s"${STimeUtil.getCurrentDateTime()} ${this.getClass.getSimpleName}.fit, dataDF.size:${dataDF.count()}")
    println(s"${STimeUtil.getCurrentDateTime()} ${this.getClass.getSimpleName}.fit, dataDF.user.size:${dataDF.select(userColumnName).distinct().count()}")
    println(s"${STimeUtil.getCurrentDateTime()} ${this.getClass.getSimpleName}.fit, dataDF.item.size:${dataDF.select(itemColumnName).distinct().count()}")

    // 物品之间的转移权重
    val transferWeightRDD: RDD[(String, Seq[(String, Double)])] = dataDF.rdd
      .map(x => (x.getAs[String](userColumnName), (x.getAs[String](timestampColumnName), x.getAs[String](itemColumnName), x.getAs[Double](ratingColumnName))))
      .groupByKey()
      .filter(_._2.size >= 2)
      .flatMap(x => {
        val buffer = ArrayBuffer[((String, String), Double)]()
        val itemSeq = x._2.toSeq.sortBy(_._1).map(y => (y._2, y._3))
        for (i <- 0.until(itemSeq.size - 1)) {
          buffer.+=(
            (
              (itemSeq(i)._1, itemSeq(i + 1)._1), itemSeq(i + 1)._2)
          )
        }
        buffer
      })
      .reduceByKey(_ + _)
      .filter(row => !row._1._1.equals(row._1._2))
      .map(row => (row._1._1, (row._1._2, row._2)))
      .groupByKey()
      .map(row => (row._1, row._2.toSeq))
      .persist(StorageLevel.MEMORY_AND_DISK)
    println(s"${STimeUtil.getCurrentDateTime()} ${this.getClass.getSimpleName}.fit, transferWeightRDD.count:${transferWeightRDD.count()}")

    this.realRandomWalkPaths = 0.until(walkEpoch).map(i => {
      var walkPath: RDD[Seq[String]] = transferWeightRDD.map(row => Seq(row._1))
      var j = 1
      var preWalkPath: RDD[Seq[String]] = null
      while (j < walkLength) {
        walkPath.cache()

        preWalkPath = walkPath
        println(s"${STimeUtil.getCurrentDateTime()} ${this.getClass.getSimpleName}.fit, epoch:${i}, iter:${j}, preWalkPath:${preWalkPath.first().mkString(",")}")

        walkPath = walkPath.map(row => (row.last, row))
          .leftOuterJoin(transferWeightRDD)
          .map(row => {
            if (row._2._2.isDefined) {
              row._2._1.++(Seq(randomChoice(row._2._2.get)))
            } else {
              row._2._1
            }
          })
        j += 1
        println(s"${STimeUtil.getCurrentDateTime()} ${this.getClass.getSimpleName}.fit, finish walk, epoch:${i}, iter:${j}")
        preWalkPath.unpersist(blocking = false)
      }
      walkPath
    })
      .reduce(_.union(_))
      .persist(StorageLevel.MEMORY_AND_DISK)
    println(s"${STimeUtil.getCurrentDateTime()} ${this.getClass.getSimpleName}.fit, realRandomWalkPaths.count:${realRandomWalkPaths.count()}")

    this
  }

  /**
   * 获取Pair对
   *
   * @return
   */
  def generatePair(): DataFrame = {
    val spark = this.spark
    import spark.implicits._

    this.realRandomWalkPaths
      .flatMap(seq => {
        val buffer = new ArrayBuffer[(String, String)]()
        seq.indices.foreach(i => {
          val i1 = seq(i)
          1.to(windowSize)
            .foreach(j => {
              if ((i - j) >= 0) {
                val i2 = seq(i - j)
                if (!i1.equals(i2)) {
                  buffer.append((i1, i2))
                }
              }
              if ((i + j) <= (seq.size - 1)) {
                val i2 = seq(i + j)
                if (!i1.equals(i2)) {
                  buffer.append((i1, i2))
                }
              }
            })
        })
        buffer
      })
      .toDF("target_item", "context_item")
  }

  override def getItemEmbedding(vectorAsString: Boolean): DataFrame = ???

}
