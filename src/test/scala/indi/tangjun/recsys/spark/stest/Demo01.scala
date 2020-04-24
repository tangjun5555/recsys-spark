package indi.tangjun.recsys.spark.stest

import org.apache.spark.ml.linalg.Vectors

import scala.util.Random


/**
 * author: tangj
 * time: 2020/4/22 17:03
 * description:
 */
object Demo01 {

  def main(args: Array[String]): Unit = {

//    val numFeatures = (621 - 1) / 4
//    println(numFeatures)

    println(1.toDouble == 1.0)

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
