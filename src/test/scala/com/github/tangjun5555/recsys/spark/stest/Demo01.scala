package com.github.tangjun5555.recsys.spark.stest

import org.apache.spark.ml.linalg.Vectors

import scala.collection.mutable.ArrayBuffer
import scala.util.Random


/**
 * author: tangj
 * time: 2020/4/22 17:03
 * description:
 */
object Demo01 {

  private def randomSample(values: Array[(Long, Double)]): Long = {
    var r = Random.nextDouble() * values.map(_._2).sum
    println(r)
    var result = values(0)._1
    for (i <- values.indices if r > 0.0) {
      r = r - values(i)._2
      result = values(i)._1
    }
    result
  }

  def main(args: Array[String]): Unit = {
    val v1 = Array("a", "b")
    val v2 = Array("c", "d")
    val v3 = v1 ++ v2
    println(v3.mkString(","))


//    val pathBuffer = new ArrayBuffer[Long]()
////    var path: Array[Long] = Array.empty[Long]
////    pathBuffer.append(path: _*)
////    println(pathBuffer.mkString(","))
////    println(path.mkString(","))
//    pathBuffer.append(10L)

//    println(pathBuffer.mkString(","))

//    val values = Array(
//      (1L, 10.0)
//      , (2L, 5.0)
//      , (3L, 20.0)
//    )
//    println(randomSample(values))
//    println(randomSample(values))
//    println(randomSample(values))

//    println(0.000 == 0.0)

//    val v1: Array[Int] = Array(1, 2).++(Array(10, 11))
//    val v2: Seq[Int] = Seq(1, 2).++(Seq(10, 11))
//    println(v1.mkString(","))
//    println(v2.mkString(","))

//    val numFeatures = (621 - 1) / 4
//    println(numFeatures)

//    println(1.toDouble == 1.0)

//    val v1 = Vectors.sparse(10, Seq((1, 1.0), (9, 1.0)))
//    println(v1.size)
//
//    val v2 = Vectors.dense(
//      Array.fill(10 + 1)(0.0) ++
//        Array.fill(10 * 8)(Random.nextGaussian() * 0.1)
//    )
//    println(v2.size)

//    println("-1".toInt)
//    println("+1".toInt)

////    println(Array.fill(5)(0.0).mkString(","))
//
//    val gradient = Vectors.zeros(10)
//    gradient match {
//      case vec: DenseVector =>
//        val cumValues = vec.values
//
//            for (f <- 0 until 10) {
//              cumValues(f) += 1.0
//            }
//      case _ =>
//        throw new IllegalArgumentException(
//          s"cumulateGradient only supports adding to a dense vector but got type.")
//    }
//    println(gradient)
  }

}
