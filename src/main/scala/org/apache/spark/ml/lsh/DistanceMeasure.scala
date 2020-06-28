package org.apache.spark.ml.lsh

import com.github.tangjun5555.recsys.spark.util.VectorUtil
import org.apache.spark.ml.linalg.{Vectors, Vector}

/**
 * author: tangj
 * time: 2020/6/21 11:42
 * description:
 */
private[lsh] sealed abstract class DistanceMeasure extends Serializable {

  /**
   * Compute distance between vectors
   *
   */
  def compute(v1: Vector, v2: Vector): Double

}

private[lsh] object InnerProductDistance extends DistanceMeasure {

  def compute(v1: Vector, v2: Vector): Double = {
    val dotProduct = VectorUtil.dot(v1, v2)
    dotProduct
  }

}

private[lsh] object CosineSimilarityDistance extends DistanceMeasure {

  def compute(v1: Vector, v2: Vector): Double = {
    val dotProduct = VectorUtil.dot(v1, v2)
    val norms = Vectors.norm(v1, 2) * Vectors.norm(v2, 2)
    assert(norms > 0)
    dotProduct / norms
  }

}

private[lsh] object JaccardSimilarityDistance extends DistanceMeasure {

  def compute(v1: Vector, v2: Vector): Double = {
    val t1 = v1.toSparse.indices.toSet
    val t2 = v2.toSparse.indices.toSet
    val n1 = t1.intersect(t2).size
    val n2 = t1.union(t2).size
    assert(n2 > 0)
    n1.toDouble / n2
  }

}
