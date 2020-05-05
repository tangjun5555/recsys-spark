package indi.tangjun.recsys.spark.`match`

import org.apache.spark.sql.DataFrame

/**
 * author: tangj 1844250138@qq.com
 * time: 2020/5/5 18:21
 * description:
 */
class Node2Vec extends ItemEmbedding {

  case class NodeAttr(
                       var neighbors: Array[(Long, Double)] = Array.empty[(Long, Double)],
                       var path: Array[Long] = Array.empty[Long]
                     ) extends Serializable

  case class EdgeAttr(
                       var dstNeighbors: Array[Long] = Array.empty[Long],
                       var J: Array[Int] = Array.empty[Int],
                       var q: Array[Double] = Array.empty[Double]
                     ) extends Serializable

  override def getItemEmbedding(vectorAsString: Boolean): DataFrame = ???
}
