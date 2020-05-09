package indi.tangjun.recsys.spark.`match`

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.ml.feature.Word2VecModel
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

  private var vectorColumnName = "vector"

  def setVectorColumnName(value: String): this.type = {
    this.vectorColumnName = value
    this
  }

  def getVectorColumnName(): String = {
    this.vectorColumnName
  }

  private var node2id: RDD[(String, Long)] = _

  private var indexedEdges: RDD[Edge[EdgeAttr]] = _
  private var indexedNodes: RDD[(VertexId, NodeAttr)] = _
  private var graph: Graph[NodeAttr, EdgeAttr] = _

  private var randomWalkPaths: RDD[(Long, ArrayBuffer[Long])] = _

  private var word2Vec: Word2VecModel = _

  private var itemVectorDF: DataFrame = _

  def fit(rawDataDF: DataFrame): this.type = {



    this
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
        .withColumnRenamed("vector", itemColumnName)
    }
  }

}
