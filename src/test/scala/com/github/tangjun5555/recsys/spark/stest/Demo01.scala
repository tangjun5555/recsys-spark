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

  /**
   * 处理连续特征
   *
   * @param value
   * @return
   */
  def transformDense(value: Double): Int = {
    if (value < 0.0) {
      0
    } else if (value == 0.0) {
      1
    } else if (value < 2.0) {
      2
    } else {
      3 + Math.pow(Math.log(value), 2.0).toInt
    }
  }

  def main(args: Array[String]): Unit = {

    val dt = "20200602"
    println(dt.slice(0, 4))
    println(dt.slice(4, 6))
    println(dt.slice(6, 8))

//    val buffer = ArrayBuffer[(String, Int)]()
//    buffer.append(
//      ("a", 1)
//    )
//    buffer.append(
//      ("b", 2)
//    )
//    buffer.append(
//      ("c", 3)
//    )
//    buffer.append(
//      ("d", 4)
//    )
//    println(buffer.mkString(","))

//    println(transformDense(2.0))
//    println(transformDense(2.1))
//    println(transformDense(2.8))
//    println(transformDense(3.2))
//    println(transformDense(3.8))
//
//    println(4.7.toInt)


//    println(System.getProperties.getProperty("os.name"))
//    val pathBuffer = new ArrayBuffer[Long]()
//    println(pathBuffer.isEmpty)
//    println(pathBuffer.length)
//    println(pathBuffer.size)
//
//    pathBuffer.append(1L)
//    println(pathBuffer.isEmpty)
//    println(pathBuffer.length)
//    println(pathBuffer.size)

//    val t1 = Seq("a,b", "c,d").flatMap(_.split(","))
//    println(t1.mkString(","))


//    val v1 = Array("a", "b")
//    val v2 = Array("c", "d")
//    val v3 = v1 ++ v2
//    println(v3.mkString(","))


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
