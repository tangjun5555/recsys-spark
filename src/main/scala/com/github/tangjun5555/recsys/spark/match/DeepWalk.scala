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

  /**
   * 物品的唯一映射
   */
  private var itemId2IndexRDD: RDD[(String, Long)] = _

  private var realRandomWalkPaths: RDD[(Long, ArrayBuffer[Long])] = _

  private var word2Vec: CustomWord2VecModel = _

  private var itemVectorDF: DataFrame = _

  def fit(rawDataDF: DataFrame): this.type = {
    val spark = rawDataDF.sparkSession
    this.spark = spark

    this.dataDF = rawDataDF.select(userColumnName, itemColumnName, ratingColumnName, timestampColumnName)
      .filter(s"${ratingColumnName}>0.0")
      .distinct()
      .persist(StorageLevel.MEMORY_AND_DISK)
    dataDF.show(30, false)

    // 统计基本信息
    println(s"[${this.getClass.getSimpleName}.fit] dataDF.size:${dataDF.count()}")
    println(s"[${this.getClass.getSimpleName}.fit] dataDF.user.size:${dataDF.select(userColumnName).distinct().count()}")
    println(s"[${this.getClass.getSimpleName}.fit] dataDF.item.size:${dataDF.select(itemColumnName).distinct().count()}")

    this.itemId2IndexRDD = dataDF.rdd.map(x => x.getAs[String](itemColumnName))
      .distinct()
      .zipWithIndex()
      .persist(StorageLevel.MEMORY_AND_DISK)
    println(itemId2IndexRDD.first())

    // 物品之间的转移权重
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

    // 生成图的顶点
    val graphNodes: RDD[(Long, NodeAttr)] = inputTriplets
      .map(x => (x._1, Array((x._2, x._3))))
      .reduceByKey(_ ++ _)
      .map(x => (x._1, NodeAttr(neighbors = x._2)))
      .persist(StorageLevel.MEMORY_AND_DISK)
    println(graphNodes.first())

    // 生成图的边
    val graphEdges: RDD[Edge[EdgeAttr]] = graphNodes
      .flatMap { case (srcId, nodeAttr) =>
        nodeAttr.neighbors.map { case (dstId, _) =>
          Edge(srcId, dstId, EdgeAttr())
        }
      }
      .persist(StorageLevel.MEMORY_AND_DISK)
    println(graphEdges.first())

    val graph: Graph[NodeAttr, EdgeAttr] = Graph(graphNodes, graphEdges, defaultVertexAttr = NodeAttr())
      .mapTriplets { edgeTriplet: EdgeTriplet[NodeAttr, EdgeAttr] =>
        edgeTriplet.attr.dstNeighbors = edgeTriplet.dstAttr.neighbors
        edgeTriplet.attr
      }
      .cache()

    val edge2Attr: RDD[(String, EdgeAttr)] = graph.triplets
      .map { edgeTriplet =>
        (s"${edgeTriplet.srcId}${edgeTriplet.dstId}", edgeTriplet.attr)
      }
      .persist(StorageLevel.MEMORY_AND_DISK)

    this.realRandomWalkPaths = spark.sparkContext.union(
      0.until(walkEpoch)
        .map(i => {
          val buffer = new ArrayBuffer[RDD[(Long, ArrayBuffer[Long])]]()
          val randomWalk: RDD[(Long, ArrayBuffer[Long])] = graph.vertices
            .map { case (vertexId: Long, nodeAttr: NodeAttr) =>
              val pathBuffer = new ArrayBuffer[Long]()
              pathBuffer.append(vertexId)
              if (!nodeAttr.neighbors.isEmpty) {
                pathBuffer.append(randomChoice(nodeAttr.neighbors))
              }
              (vertexId, pathBuffer)
            }
            .filter(_._2.size >= 2)
          buffer.append(randomWalk)
          for (j <- 0.until(this.walkLength - 2)) {
            val randomWalk = buffer(buffer.size - 1)
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
            buffer.append(randomWalk)
            println(s"[${this.getClass.getSimpleName}.fit] finish walk, epoch:${i}, iter:${j}")
          }
          buffer(buffer.size - 1)
        })
    )
      .persist(StorageLevel.MEMORY_AND_DISK)

    println(s"序列的数量:${this.realRandomWalkPaths.count()}")
    println(this.realRandomWalkPaths.take(10).mkString("Array(", ", ", ")"))

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

    val index2ItemIdRDD: RDD[(Long, String)] = itemId2IndexRDD.map(x => (x._2, x._1))
      .persist(StorageLevel.MEMORY_AND_DISK)
    this.realRandomWalkPaths
      .map(_._2.toSeq)
      .flatMap(seq => {
        val buffer = new ArrayBuffer[(Long, Long)]()
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
      .join(index2ItemIdRDD)
      .map(x => x._2)
      .join(index2ItemIdRDD)
      .map(x => x._2)
      .toDF("target_item", "context_item")
  }

  override def getItemEmbedding(vectorAsString: Boolean): DataFrame = {
    val spark = this.spark
    import spark.implicits._

    this.word2Vec = new CustomWord2Vec()
      .setNumIterations(20)
      .setVectorSize(32)
      .setLearningRate(0.025)
      .setMaxSentenceLength(this.walkLength)
      .setWindowSize(windowSize)
      .setMinCount(0)
      .setNumPartitions(spark.conf.get("spark.default.parallelism").toInt / 20)
      .setSeed(555L)
      .fit(realRandomWalkPaths.map(x => x._2.map(_.toString)))

    val index2ItemIdRDD: RDD[(Long, String)] = itemId2IndexRDD.map(x => (x._2, x._1))
    this.itemVectorDF = spark.sparkContext.makeRDD(this.word2Vec.getVectors.toSeq.map(x => (x._1.toLong, x._2)))
      .join(index2ItemIdRDD)
      .map(x => (x._2._2, x._2._1))
      .toDF("word", "vector")

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
