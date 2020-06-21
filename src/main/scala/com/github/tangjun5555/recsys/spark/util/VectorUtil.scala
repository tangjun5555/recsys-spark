package com.github.tangjun5555.recsys.spark.util

import breeze.linalg.{DenseVector => BDV, SparseVector => BSV, Vector => BV}
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vector}
import org.apache.spark.ml.linalg.{DenseVector => MLDenseVector, SparseVector => MLSparseVector, Vector => MLVector, Vectors => MLVectors}

/**
 * author: tangj
 * time: 2020/4/26 17:29
 * description:
 */
object VectorUtil {

  /**
   * Compute the dot product between two vectors
   */
  def dot(x: MLVector, y: MLVector): Double = {
    var result = 0.0
    val v1 = x.toDense.values
    val v2 = y.toDense.values

    assert(v1.length ==  v2.length)

    for (i <- v1.indices) {
      result += v1(i) * v2(i)
    }
    result
  }

  def breeze2MLVector(breezeVector: BV[Double]): MLVector = {
    breezeVector match {
      case v: BDV[Double] =>
        if (v.offset == 0 && v.stride == 1 && v.length == v.data.length) {
          new MLDenseVector(v.data)
        } else {
          new MLDenseVector(v.toArray)
        }
      case v: BSV[Double] =>
        if (v.index.length == v.used) {
          new MLSparseVector(v.length, v.index, v.data)
        } else {
          new MLSparseVector(v.length, v.index.slice(0, v.used), v.data.slice(0, v.used))
        }
      case v: BV[_] =>
        sys.error("Unsupported Breeze vector type: " + v.getClass.getName)
    }
  }

  def toBreeze(mllibVec: Vector): BV[Double] = new BDV[Double](mllibVec.toDense.values)

  def fromBreeze(breezeVector: BV[Double]): Vector = {
    breezeVector match {
      case v: BDV[Double] =>
        if (v.offset == 0 && v.stride == 1 && v.length == v.data.length) {
          new DenseVector(v.data)
        } else {
          new DenseVector(v.toArray) // Can't use underlying array directly, so make a new one
        }
      case v: BSV[Double] =>
        if (v.index.length == v.used) {
          new SparseVector(v.length, v.index, v.data)
        } else {
          new SparseVector(v.length, v.index.slice(0, v.used), v.data.slice(0, v.used))
        }
      case v: BV[_] =>
        sys.error("Unsupported Breeze vector type: " + v.getClass.getName)
    }
  }

  def concatMLVector(v1: MLVector, v2: MLVector): MLVector = {
    if (v1.isInstanceOf[MLSparseVector] && v2.isInstanceOf[MLSparseVector]) {
      MLVectors.sparse(v1.size + v2.size, v1.toSparse.indices.++(v2.toSparse.indices.map(i => v1.size + i)), v1.toSparse.values.++(v2.toSparse.values))
    } else {
      MLVectors.dense(v1.toDense.values.++(v2.toDense.values))
    }
  }

}
