package indi.tangjun.recsys.spark.`match`

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ArrayBuffer

/**
 * author: tangj 1844250138@qq.com
 * time: 2020/5/5 18:21
 * description:
 */
class Node2Vec extends ItemEmbedding {

  case class NodeAttr(
                       var neighbors: Array[(Long, Double)] = Array.empty[(Long, Double)],  // 顶点的邻居和对应的权重
                       var path: Array[Long] = Array.empty[Long]  // 顶点随机游走的列表，初始值为空
                     ) extends Serializable

  case class EdgeAttr(
                       var dstNeighbors: Array[Long] = Array.empty[Long],  // 终点的邻居，初始值为空
                       var J: Array[Int] = Array.empty[Int],   // Alias抽样方法的J值，初始值为空
                       var q: Array[Double] = Array.empty[Double] // Alias抽样方法的q值，初始值为空
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

  var node2id: RDD[(String, Long)] = _

  var indexedEdges: RDD[Edge[EdgeAttr]] = _
  var indexedNodes: RDD[(VertexId, NodeAttr)] = _
  var graph: Graph[NodeAttr, EdgeAttr] = _

  var randomWalkPaths: RDD[(Long, ArrayBuffer[Long])] = _

  def fit(rawDataDF: DataFrame) = {

  }

  override def getItemEmbedding(vectorAsString: Boolean): DataFrame = ???
}
