package indi.tangjun.recsys.spark.`match`

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.{Edge, EdgeTriplet, Graph, VertexId}
import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
 * author: tangj 1844250138@qq.com
 * time: 2020/5/5 18:21
 * description:
 *
 * 算法流程
 * 1、收集用户行为序列
 * 2、构建物品关系图
 * 3、DFS与BFS产生物品序列
 * 4、通过word2vec产生物品向量
 *
 */
class Node2Vec extends ItemEmbedding {

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
    this.itemColumnName = itemColumnName
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

  private var walkLength: Int = 50

  private var p: Double = 1.0

  private var q: Double = 1.0

  private var word2Vec: Word2VecModel = _

  private var itemVectorDF: DataFrame = _

  def fit(rawDataDF: DataFrame): this.type = {
    val spark = rawDataDF.sparkSession
    this.spark = spark
    import spark.implicits._

    val dataDF = rawDataDF.select(userColumnName, itemColumnName, ratingColumnName, timestampColumnName)
      .filter(s"${ratingColumnName}>0.0")
      .distinct()
      .persist(StorageLevel.MEMORY_AND_DISK)
    this.dataDF = dataDF

    // 统计基本信息
    println(s"[${this.getClass.getSimpleName}.fit] dataDF.size:${dataDF.count()}")
    println(s"[${this.getClass.getSimpleName}.fit] dataDF.user.size:${dataDF.select(userColumnName).distinct().count()}")
    println(s"[${this.getClass.getSimpleName}.fit] dataDF.item.size:${dataDF.select(itemColumnName).distinct().count()}")

    val itemId2Index: Array[(String, Long)] = dataDF.rdd.map(x => x.getAs[String](itemColumnName))
      .distinct()
      .zipWithIndex
      .collect()
    val itemId2IndexBroadcast: Broadcast[Map[String, Long]] = spark.sparkContext.broadcast(itemId2Index.toMap)
    val index2ItemIdBroadcast: Broadcast[Map[Long, String]] = spark.sparkContext.broadcast(itemId2Index.map(x => (x._2, x._1)).toMap)

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
      .map(x => {
        val itemId2IndexValue: Map[String, Long] = itemId2IndexBroadcast.value
        (itemId2IndexValue(x._1._1), itemId2IndexValue(x._1._2), x._2)
      })

    val graphNodes: RDD[(VertexId, NodeAttr)] = inputTriplets
      .map(x => (x._1, Array((x._2, x._3))))
      .reduceByKey(_ ++ _)
      .map(x => (x._1, NodeAttr(neighbors = x._2)))
      .persist(StorageLevel.MEMORY_AND_DISK)

    val graphEdges: RDD[Edge[EdgeAttr]] = graphNodes
      .flatMap { case (srcId, nodeAttr) =>
        nodeAttr.neighbors.map { case (dstId, _) =>
          Edge(srcId, dstId, EdgeAttr())
        }
      }

    val graph: Graph[NodeAttr, EdgeAttr] = Graph(graphNodes, graphEdges)
      .mapVertices[NodeAttr] { case (vertexId: Long, nodeAttr: NodeAttr) =>
        nodeAttr.path = Array(vertexId, randomChoice(nodeAttr.neighbors))
        nodeAttr
      }
      .mapTriplets { edgeTriplet: EdgeTriplet[NodeAttr, EdgeAttr] =>
        edgeTriplet.attr.srcNeighbors = edgeTriplet.srcAttr.neighbors
        edgeTriplet.attr.dstNeighbors = edgeTriplet.dstAttr.neighbors
        edgeTriplet.attr
      }
      .persist(StorageLevel.MEMORY_AND_DISK)

    val edge2Attr: RDD[(String, EdgeAttr)] = graph.triplets.map { edgeTriplet =>
      (s"${edgeTriplet.srcId}${edgeTriplet.dstId}", edgeTriplet.attr)
    }
      .persist(StorageLevel.MEMORY_AND_DISK)

    var randomWalkPaths: RDD[(Long, ArrayBuffer[Long])] = null

    for (i <- 0.until(this.walkEpoch)) {
      var prevWalk: RDD[(Long, ArrayBuffer[Long])] = null
      var randomWalk: RDD[(VertexId, ArrayBuffer[VertexId])] = graph.vertices.map { case (vertexId: Long, nodeAttr: NodeAttr) =>
        val pathBuffer = new ArrayBuffer[Long]()
        pathBuffer.append(vertexId)
        pathBuffer.append(randomChoice(nodeAttr.neighbors))
        (vertexId, pathBuffer)
      }
        .persist(StorageLevel.MEMORY_AND_DISK)

      for (j <- 0.until(this.walkLength - 2)) {
        prevWalk = randomWalk
        randomWalk = randomWalk.map { case (srcNodeId, pathBuffer) =>
          val prevNodeId = pathBuffer(pathBuffer.length - 2)
          val currentNodeId = pathBuffer(pathBuffer.length - 1)
          (s"$prevNodeId$currentNodeId", (srcNodeId, pathBuffer))
        }
          .join(edge2Attr)
          .map { case (_, ((srcNodeId, pathBuffer), edgeAttr)) =>
            val srcNeighbors: Array[(VertexId, Double)] = edgeAttr.srcNeighbors
            val dstNeighbors: Array[(VertexId, Double)] = edgeAttr.dstNeighbors
            val neighbors_ : Array[(Long, Double)] = dstNeighbors.map { case (dstNeighborId, weight) =>
              var unnormProb = weight / q
              if (srcNodeId == dstNeighborId) {
                unnormProb = weight / p
              } else if (srcNeighbors.exists(_._1 == dstNeighborId)) {
                unnormProb = weight
              }
              (dstNeighborId, unnormProb)
            }
            val nextNodeId = randomChoice(neighbors_)
            pathBuffer.append(nextNodeId)
            (srcNodeId, pathBuffer)
          }
          .persist(StorageLevel.MEMORY_AND_DISK)
        prevWalk.unpersist(blocking = false)
      }

      if (randomWalkPaths != null) {
        val prevRandomWalkPaths = randomWalkPaths
        randomWalkPaths = randomWalkPaths.union(randomWalk).persist(StorageLevel.MEMORY_AND_DISK)
        randomWalkPaths.first
        prevRandomWalkPaths.unpersist(blocking = false)
      } else {
        randomWalkPaths = randomWalk
      }
    }

    this.word2Vec = new Word2Vec()
      .setNumIterations(20)
      .setVectorSize(32)
      .setLearningRate(0.025)
      .setMaxSentenceLength(this.walkLength)
      .setWindowSize(3)
      .setMinCount(0)
      .setNumPartitions(spark.conf.get("spark.default.parallelism").toInt / 20)
      .setSeed(555L)
      .fit(randomWalkPaths.map(x => {
        x._2.map(y => {
          val index2ItemIdValue = index2ItemIdBroadcast.value
          index2ItemIdValue(y)
        })
      }))

    this.itemVectorDF = this.word2Vec.getVectors.toSeq.toDF("word", "vector")

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
        .withColumnRenamed("vector", vectorColumnName)
    }
  }

}
