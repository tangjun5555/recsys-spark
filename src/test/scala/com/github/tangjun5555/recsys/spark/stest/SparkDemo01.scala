package com.github.tangjun5555.recsys.spark.stest

import com.github.tangjun5555.recsys.spark.util.SparkUtil
import org.apache.log4j.Level
import org.apache.spark.rdd.RDD

/**
 * author: tangj
 * time: 2020/4/24 18:53
 * description:
 */
object SparkDemo01 {

  def main(args: Array[String]): Unit = {
    val spark = SparkUtil.getLocalSparkSession(this.getClass.getSimpleName, Level.ERROR)

      val dataRDD: RDD[Int] = spark.sparkContext.parallelize(0.until(10))

    0.until(5).foreach(x =>
      println(dataRDD.sample(false, 0.2).collect().mkString(","))
    )

//    0.until(5).foreach(x =>
//      println(dataRDD.sample(true, 0.2).collect().mkString(","))
//    )

//    val sample: RDD[Int] = 0.until(10).map(i => {
//        dataRDD.sample(false, 0.5)
//    }).reduce(_.union(_))
//    val count = sample.map(x => (x, 1))
//        .reduceByKey(_ + _)
//    count.collect().sortBy(_._1).foreach(x => println(s"${x._1}:${x._2}"))

    spark.stop()
  }

}
