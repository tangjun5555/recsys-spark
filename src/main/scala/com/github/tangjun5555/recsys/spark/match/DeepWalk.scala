package com.github.tangjun5555.recsys.spark.`match`

import org.apache.spark.graphx.{Edge, EdgeTriplet, Graph, VertexId}
import org.apache.spark.mllib.{CustomWord2Vec, CustomWord2VecModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
 * author: tangj 1844250138@qq.com
 * time: 2020/4/11 12:04
 * description:
 *
 * 算法流程
 * 1、收集用户行为序列
 * 2、构建物品关系图
 * 3、随机游走产生物品序列
 * 4、通过word2vec产生物品向量
 *
 */
class DeepWalk extends ItemEmbedding {

  private def randomChoice(values: Array[(Long, Double)]): Long = {
    var r = Random.nextDouble() * values.map(_._2).sum
    var result = values(0)._1
    for (i <- values.indices if r > 0.0) {
      r = r - values(i)._2
      result = values(i)._1
    }
    result
  }

  private case class NodeAttr(
                               var neighbors: Array[(Long, Double)] = Array.empty[(Long, Double)], // 顶点的邻居和对应的权重
                               var path: Array[Long] = Array.empty[Long] // 顶点随机游走的列表，初始值为空
                             ) extends Serializable

  private case class EdgeAttr(
                               var srcNeighbors: Array[(Long, Double)] = Array.empty[(Long, Double)], // 起点的邻居和对应的权重
                               var dstNeighbors: Array[(Long, Double)] = Array.empty[(Long, Double)] // 终点的邻居和对应的权重
                             ) extends Serializable

  private var spark: SparkSession = _

  /**
   * 保留训练数据
   */
  private var dataDF: DataFrame = _

  private var userColumnName: String = "user"

  def setUserColumnName(value: String): this.type = {
    this.userColumnName = value
    this
  }

  def getUserColumnName(): String = {
    this.userColumnName
  }

  private var itemColumnName: String = "item"

  def setItemColumnName(value: String): this.type = {
    this.itemColumnName = value
    this
  }

  def getItemColumnName(): String = {
    this.itemColumnName
  }

  private var ratingColumnName = "rating"

  def setRatingColumnName(value: String): this.type = {
    this.ratingColumnName = value
    this
  }

  def getRatingColumnName(): String = {
    this.ratingColumnName
  }

  private var timestampColumnName: String = "timestamp"

  def setTimestampColumnName(value: String): this.type = {
    this.timestampColumnName = value
    this
  }

  def getTimestampColumnName(): String = {
    this.timestampColumnName
  }

  private var vectorColumnName = "vector"

  def setVectorColumnName(value: String): this.type = {
    this.vectorColumnName = value
    this
  }

  def getVectorColumnName(): String = {
    this.vectorColumnName
  }

  private var walkEpoch: Int = 10

  def setWalkEpoch(value: Int): this.type = {
    this.walkEpoch = value
    this
  }

  def getWalkEpoch(): Int = {
    this.walkEpoch
  }

  private var walkLength: Int = 30

  def setWalkLength(value: Int): this.type = {
    this.walkLength = value
    this
  }

  def getWalkLength(): Int = {
    this.walkLength
  }

  private var word2Vec: CustomWord2VecModel = _

  private var itemVectorDF: DataFrame = _

  def fit(rawDataDF: DataFrame): this.type = {
    val spark = rawDataDF.sparkSession
    this.spark = spark
    import spark.implicits._

    val dataDF = rawDataDF.select(userColumnName, itemColumnName, ratingColumnName, timestampColumnName)
      .filter(s"${ratingColumnName}>0.0")
      .distinct()
      .persist(StorageLevel.MEMORY_AND_DISK)
    dataDF.show(30, false)
    this.dataDF = dataDF

    // 统计基本信息
    println(s"[${this.getClass.getSimpleName}.fit] dataDF.size:${dataDF.count()}")
    println(s"[${this.getClass.getSimpleName}.fit] dataDF.user.size:${dataDF.select(userColumnName).distinct().count()}")
    println(s"[${this.getClass.getSimpleName}.fit] dataDF.item.size:${dataDF.select(itemColumnName).distinct().count()}")

    val itemId2IndexRDD: RDD[(String, Long)] = dataDF.rdd.map(x => x.getAs[String](itemColumnName))
      .distinct()
      .zipWithIndex()
      .persist(StorageLevel.MEMORY_AND_DISK)
    println(itemId2IndexRDD.first())

    val inputTriplets: RDD[(VertexId, VertexId, Double)] = dataDF.rdd
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
      .map(x => (x._1._1, (x._1._2, x._2)))
      .join(itemId2IndexRDD)
      .map(x => (x._2._1._1, (x._2._2, x._2._1._2)))
      .join(itemId2IndexRDD)
      .map(x => (x._2._1._1, x._2._2, x._2._1._2))

    val graphNodes: RDD[(Long, NodeAttr)] = inputTriplets
      .map(x => (x._1, Array((x._2, x._3))))
      .reduceByKey(_ ++ _)
      .map(x => (x._1, NodeAttr(neighbors = x._2)))
      .cache()
    println(graphNodes.first())

    val graphEdges: RDD[Edge[EdgeAttr]] = graphNodes
      .flatMap { case (srcId, nodeAttr) =>
        nodeAttr.neighbors.map { case (dstId, _) =>
          Edge(srcId, dstId, EdgeAttr())
        }
      }
      .cache()
    println(graphEdges.first())

    val graph: Graph[NodeAttr, EdgeAttr] = Graph(graphNodes, graphEdges, defaultVertexAttr = NodeAttr())
      .mapTriplets { edgeTriplet: EdgeTriplet[NodeAttr, EdgeAttr] =>
        edgeTriplet.attr.dstNeighbors = edgeTriplet.dstAttr.neighbors
        edgeTriplet.attr
      }
      .cache()

    val edge2Attr: RDD[(String, EdgeAttr)] = graph.triplets.map { edgeTriplet =>
      (s"${edgeTriplet.srcId}${edgeTriplet.dstId}", edgeTriplet.attr)
    }
      .persist(StorageLevel.MEMORY_AND_DISK)

    val realRandomWalkPaths: RDD[(Long, ArrayBuffer[Long])] = 0.until(walkEpoch).map(i => {

      var preRandomWalk: RDD[(VertexId, ArrayBuffer[VertexId])] = null
      var randomWalk: RDD[(VertexId, ArrayBuffer[VertexId])] = graph.vertices
        .map { case (vertexId: Long, nodeAttr: NodeAttr) =>
          val pathBuffer = new ArrayBuffer[Long]()
          pathBuffer.append(vertexId)
          if (!nodeAttr.neighbors.isEmpty) {
            pathBuffer.append(randomChoice(nodeAttr.neighbors))
          }
          (vertexId, pathBuffer)
        }
        .filter(_._2.size >= 2)
        .cache()

      for (j <- 0.until(this.walkLength - 2)) {
        preRandomWalk = randomWalk
        randomWalk = randomWalk
          .map { case (srcNodeId, pathBuffer) =>
            val prevNodeId = pathBuffer(pathBuffer.length - 2)
            val currentNodeId = pathBuffer(pathBuffer.length - 1)
            (s"$prevNodeId$currentNodeId", (srcNodeId, pathBuffer))
          }
          .join(edge2Attr)
          .map { case (_, ((srcNodeId, pathBuffer), edgeAttr)) =>
            val dstNeighbors: Array[(VertexId, Double)] = edgeAttr.dstNeighbors
            if (!dstNeighbors.isEmpty) {
              val nextNodeId = randomChoice(edgeAttr.dstNeighbors)
              pathBuffer.append(nextNodeId)
            }
            (srcNodeId, pathBuffer)
          }

        println(s"[${this.getClass.getSimpleName}.fit] finish walk, epoch:${i}, iter:${j}")
        preRandomWalk.unpersist(false)
      }
      randomWalk
    })
      .reduce(_.union(_))
      .persist(StorageLevel.MEMORY_AND_DISK)
    println(realRandomWalkPaths.count())

    this.word2Vec = new CustomWord2Vec()
      .setNumIterations(20)
      .setVectorSize(32)
      .setLearningRate(0.025)
      .setMaxSentenceLength(this.walkLength)
      .setWindowSize(2)
      .setMinCount(0)
      .setNumPartitions(spark.conf.get("spark.default.parallelism").toInt / 20)
      .setSeed(555L)
      .fit(realRandomWalkPaths.map(x => x._2.map(_.toString)))

    val index2ItemIdRDD: RDD[(Long, String)] = itemId2IndexRDD.map(x => (x._2, x._1))
    this.itemVectorDF = spark.sparkContext.makeRDD(this.word2Vec.getVectors.toSeq.map(x => (x._1.toLong, x._2)))
      .join(index2ItemIdRDD)
      .map(x => (x._2._2, x._2._1))
      .toDF("word", "vector")

    this
  }

  def generatePair(): DataFrame = {

  }

  override def getItemEmbedding(vectorAsString: Boolean): DataFrame = {
    val spark = this.spark
    import spark.implicits._
    if (vectorAsString) {
      this.itemVectorDF
        .rdd.map(row => (row.getAs[String]("word"), row.getAs[Seq[Float]]("vector").mkString(",")))
        .toDF(itemColumnName, vectorColumnName)
    } else {
      this.itemVectorDF.withColumnRenamed("word", itemColumnName)
        .withColumnRenamed("vector", vectorColumnName)
    }
  }

  def getItemPair(rawDataDF: DataFrame): DataFrame = {
    rawDataDF
  }

}
