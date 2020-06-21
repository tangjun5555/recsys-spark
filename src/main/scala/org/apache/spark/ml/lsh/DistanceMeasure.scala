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

private[lsh] object CosineDistance extends DistanceMeasure {

  def compute(v1: Vector, v2: Vector): Double = {
    val dotProduct = VectorUtil.dot(v1, v2)
    val norms = Vectors.norm(v1, 2) * Vectors.norm(v2, 2)

    dotProduct / norms
  }

}

private[lsh] final object InnerProductDistance extends DistanceMeasure {

  def compute(v1: Vector, v2: Vector): Double = {
    val dotProduct = VectorUtil.dot(v1, v2)

    dotProduct
  }

}
