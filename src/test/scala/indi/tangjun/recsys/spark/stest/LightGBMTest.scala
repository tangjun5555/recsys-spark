package indi.tangjun.recsys.spark.stest

import indi.tangjun.recsys.spark.util.SparkUtil
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
 * author: tangj
 * time: 2020/4/24 18:06
 * description:
 */
object LightGBMTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkUtil.getLocalSparkSession(this.getClass.getSimpleName)

    val trainDF = readData(spark, "data/libsvmtools_datasets/a9a/a9a.tr")
      .persist(StorageLevel.MEMORY_AND_DISK)
    val validDF = readData(spark, "data/libsvmtools_datasets/a9a/a9a.te")
      .persist(StorageLevel.MEMORY_AND_DISK)

    val classifier = new LightGBM()
      .fit(trainDF)

//    classifier

    spark.stop()
  }

  def readData(spark: SparkSession, fileName: String): DataFrame = {
    import spark.implicits._
    spark.sparkContext.textFile(fileName)
      .map(line => {
        val items = line.trim.split(" ").toSeq
        val label = if (items.head.toInt == 1) {
          1.0
        } else {
          0.0
        }
        val features: Seq[(Int, Double)] = items.tail.map(x => (x.split(":")(0).trim.toInt - 1, x.split(":")(1).trim.toDouble))
        (label, Vectors.sparse(123, features))
      })
      .toDF("label", "features")
  }

}
