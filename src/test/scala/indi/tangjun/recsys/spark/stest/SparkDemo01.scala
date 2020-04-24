package indi.tangjun.recsys.spark.stest

import indi.tangjun.recsys.spark.util.SparkUtil
import org.apache.log4j.Level
import org.apache.spark.rdd.RDD

/**
 * author: tangj
 * time: 2020/4/24 18:53
 * description:
 */
object SparkDemo01 {

  def main(args: Array[String]): Unit = {
    val spark = SparkUtil.getLocalSparkSession(this.getClass.getSimpleName, 3, Level.ERROR)

    val dataRDD = spark.sparkContext.parallelize(0.until(100))
    val sample: RDD[Int] = 0.until(10).map(i => {
        dataRDD.sample(false, 0.5)
    }).reduce(_.union(_))
    val count = sample.map(x => (x, 1))
        .reduceByKey(_ + _)
    count.collect().sortBy(_._1).foreach(x => println(s"${x._1}:${x._2}"))

    spark.stop()
  }

}
