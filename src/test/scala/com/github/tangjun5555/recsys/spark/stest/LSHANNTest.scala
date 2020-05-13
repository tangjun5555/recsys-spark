package com.github.tangjun5555.recsys.spark.stest

import com.github.karlhigley.spark.neighbors.ANN

/**
 * author: tangj
 * time: 2020/5/6 14:51
 * description:
 */
class LSHANNTest {

  def main(args: Array[String]): Unit = {
    val annModel = new ANN(dimensions = 1000, measure = "hamming")
        .setTables(4)
        .setSignatureLength(64)
//        .train(points)
  }

}
