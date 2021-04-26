package com.github.tangjun5555.recsys.spark.example

import com.github.tangjun5555.recsys.spark.jutil.JavaTimeUtil
import com.github.tangjun5555.recsys.spark.rank.FactorizationMachine
import com.github.tangjun5555.recsys.spark.util.SparkUtil
import org.apache.log4j.Level
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
 * author: tangj
 * time: 2020/5/16 19:47
 * description:
 *
 * 数据集说明:
 *
 */
object KaggleCriteoCTRFactorizationMachine {

  val labelColumnName: String = "label"
  val denseFeatureColumnName: Seq[String] = 1.to(13).map(x => s"i${x}")
  val sparseFeatureColumnName: Seq[String] = 1.to(26).map(x => s"c${x}")

  def main(args: Array[String]): Unit = {
    val startTime = JavaTimeUtil.getCurrentDateTime

    val rawDataFile = args(0)
    val featureMapFile = args(1)
    val transformDataDFSavePath = args(2)
    val transformDataDFReadPath = args(3)

    val spark = SparkUtil.getSparkSession(this.getClass.getSimpleName, cores = 5, logLevel = Level.INFO)
    import spark.implicits._
    spark.udf.register("transformDense", transformDense _)
    spark.udf.register("transformSparse", transformSparse _)

    val sparseFeatureMap: Map[String, Seq[String]] = spark.sparkContext.textFile(featureMapFile)
      .filter(_.startsWith("C"))
      .map(_.toLowerCase)
      .map(x => {
        val items = x.split("\\|")
        assert(items.size == 2)
        (items(0), items(1).split(" ")(0))
      })
      .groupByKey()
      .map(x => (x._1, x._2.toSeq.sorted))
      .collect()
      .toMap
    println(s"sparseFeatureMap:${sparseFeatureMap}")

    val transformDataDF = if ("".equals(transformDataDFReadPath)) {
      // 读取原始数据
      val schema: StructType = StructType(
        Seq(StructField(labelColumnName, IntegerType))
          .++(denseFeatureColumnName.map(x => StructField(x, DoubleType)))
          .++(sparseFeatureColumnName.map(x => StructField(x, StringType)))
      )
      val rawDataDF: DataFrame = spark.read.schema(schema)
        .option("header", false)
        .csv(rawDataFile)
        .persist(StorageLevel.MEMORY_AND_DISK)
      rawDataDF.createTempView("rawDataDF")

      // 进行简单统计
      rawDataDF.show(30, false)
      spark.sql(
        s"""
           |select label, count(label) num
           |from rawDataDF
           |group by label
           |order by label
           |""".stripMargin)
        .show(30, false)

      // 填补缺失值
      val fillNaDataDF = rawDataDF
        .na.fill(-1.0, denseFeatureColumnName)
        .na.fill("UNKNOWN", sparseFeatureColumnName)
        .persist(StorageLevel.MEMORY_AND_DISK)
      fillNaDataDF.createTempView("fillNaDataDF")
      fillNaDataDF.show(30, false)
      rawDataDF.unpersist()

      spark.sql(
        s"""
           |select *
           |
           |  , transformDense(i1) i1_new
           |  , transformDense(i2) i2_new
           |  , transformDense(i3) i3_new
           |  , transformDense(i4) i4_new
           |  , transformDense(i5) i5_new
           |  , transformDense(i6) i6_new
           |  , transformDense(i7) i7_new
           |  , transformDense(i8) i8_new
           |  , transformDense(i9) i9_new
           |  , transformDense(i10) i10_new
           |  , transformDense(i11) i11_new
           |  , transformDense(i12) i12_new
           |  , transformDense(i13) i13_new
           |
           |  , transformSparse(c1, '${sparseFeatureMap("c1").mkString(",")}') c1_new
           |  , transformSparse(c2, '${sparseFeatureMap("c2").mkString(",")}') c2_new
           |  , transformSparse(c3, '${sparseFeatureMap("c3").mkString(",")}') c3_new
           |  , transformSparse(c4, '${sparseFeatureMap("c4").mkString(",")}') c4_new
           |  , transformSparse(c5, '${sparseFeatureMap("c5").mkString(",")}') c5_new
           |  , transformSparse(c6, '${sparseFeatureMap("c6").mkString(",")}') c6_new
           |  , transformSparse(c7, '${sparseFeatureMap("c7").mkString(",")}') c7_new
           |  , transformSparse(c8, '${sparseFeatureMap("c8").mkString(",")}') c8_new
           |  , transformSparse(c9, '${sparseFeatureMap("c9").mkString(",")}') c9_new
           |  , transformSparse(c10, '${sparseFeatureMap("c10").mkString(",")}') c10_new
           |  , transformSparse(c11, '${sparseFeatureMap("c11").mkString(",")}') c11_new
           |  , transformSparse(c12, '${sparseFeatureMap("c12").mkString(",")}') c12_new
           |  , transformSparse(c13, '${sparseFeatureMap("c13").mkString(",")}') c13_new
           |  , transformSparse(c14, '${sparseFeatureMap("c14").mkString(",")}') c14_new
           |  , transformSparse(c15, '${sparseFeatureMap("c15").mkString(",")}') c15_new
           |  , transformSparse(c16, '${sparseFeatureMap("c16").mkString(",")}') c16_new
           |  , transformSparse(c17, '${sparseFeatureMap("c17").mkString(",")}') c17_new
           |  , transformSparse(c18, '${sparseFeatureMap("c18").mkString(",")}') c18_new
           |  , transformSparse(c19, '${sparseFeatureMap("c19").mkString(",")}') c19_new
           |  , transformSparse(c20, '${sparseFeatureMap("c20").mkString(",")}') c20_new
           |  , transformSparse(c21, '${sparseFeatureMap("c21").mkString(",")}') c21_new
           |  , transformSparse(c22, '${sparseFeatureMap("c22").mkString(",")}') c22_new
           |  , transformSparse(c23, '${sparseFeatureMap("c23").mkString(",")}') c23_new
           |  , transformSparse(c24, '${sparseFeatureMap("c24").mkString(",")}') c24_new
           |  , transformSparse(c25, '${sparseFeatureMap("c25").mkString(",")}') c25_new
           |  , transformSparse(c26, '${sparseFeatureMap("c26").mkString(",")}') c26_new
           |from fillNaDataDF
           |""".stripMargin)
        .persist(StorageLevel.MEMORY_AND_DISK)
    } else {
      spark.read
        .option("header", true)
        .option("inferSchema", true.toString)
        .csv(s"${transformDataDFReadPath}/*.csv")
        .persist(StorageLevel.MEMORY_AND_DISK)
    }

    if (!"".equals(transformDataDFSavePath)) {
      transformDataDF
        .coalesce(1)
        .write
        .option("header", "true")
        .mode(SaveMode.Overwrite)
        .csv(transformDataDFSavePath)
    }

    transformDataDF.createTempView("transformDataDF")
    transformDataDF.show(30, false)

    val denseVocabSizeMap: Map[String, Int] = denseFeatureColumnName.map(x => {
      (x,
        spark.sql(
          s"""
             |select max(${x}_new)
             |from transformDataDF
             |""".stripMargin)
          .rdd.map(_.getAs[Int](0)).collect()(0) + 1
      )
    })
      .toMap
    println(s"denseVocabSizeMap:${denseVocabSizeMap.toSeq.sortBy(_._1).mkString(",")}")

    val sparseVocabSizeMap: Map[String, Int] = sparseFeatureMap.mapValues(_.size + 1)
    println(s"sparseVocabSizeMap:${sparseVocabSizeMap.toSeq.sortBy(_._1).mkString(",")}")

    val allVocabSizeMap: Map[String, Int] = denseVocabSizeMap.++(sparseVocabSizeMap)
    println(s"allVocabSizeMap:${allVocabSizeMap.toSeq.sortBy(_._1).mkString(",")}")
    val allVocabSizeMapBroadcast: Broadcast[Map[String, Int]] = spark.sparkContext.broadcast(allVocabSizeMap)

    val modelDataDF = transformDataDF.rdd.map(row => {
      val label: Double = row.getAs[Int](labelColumnName).toDouble
      val values: Map[String, Int] = denseFeatureColumnName.++(sparseFeatureColumnName).map(x => {
        (x, row.getAs[Int](s"${x}_new"))
      })
        .toMap
      (label, combineAllFeature(values, allVocabSizeMapBroadcast.value))
    })
      .zipWithIndex()
      .map(x => (x._2.toString, x._1._1, x._1._2))
      .toDF("sample_id", "label", "features")
      .persist(StorageLevel.MEMORY_AND_DISK)
    modelDataDF.createTempView("modelDataDF")
    modelDataDF.show(30, false)
    transformDataDF.unpersist()

    val Array(trainDF, validDF) = modelDataDF.randomSplit(Array(0.8, 0.2), seed = 555L)
    trainDF.persist(StorageLevel.MEMORY_AND_DISK)
    validDF.persist(StorageLevel.MEMORY_AND_DISK)

    val model = new FactorizationMachine()
      .setEpoch(20)
      .setMiniBatchFraction(0.2)
      .setFactorDim(4)
      .setRegularizationType("L1")
      .setLearningRate(0.01)
      .setOptimizer("GradientDescent")
      .fit(trainDF)

    val trainPreDF = model.predict(trainDF)
    val validPreDF = model.predict(validDF)

//    val evaluator = new AUCAndLogLossEvaluator()
//    val (trainLogLoss, trainAUC) = evaluator.evaluate(trainPreDF)
//    val (validLogLoss, validAUC) = evaluator.evaluate(validPreDF)
//
//    println(s"trainLogLoss:${trainLogLoss}, trainAUC:${trainAUC}")
//    println(s"validLogLoss:${validLogLoss}, validAUC:${validAUC}")

    val endTime = JavaTimeUtil.getCurrentDateTime
    println(s"startTime:${startTime}, endTime:${endTime}")
    Thread.sleep(1000 * 1000)
    spark.stop()
  }

  /**
   * 处理连续特征
   * 离散化
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

  /**
   * 处理离散特征
   * 缺失和频率少是两种情况
   *
   * @param value
   * @param enums
   * @return
   */
  def transformSparse(value: String, enums: String): Int = {
    val realEnums: Array[String] = enums.split(",")
    var flag = true
    var result: Int = 0
    for (i <- realEnums.indices if (flag)) {
      if (realEnums(i).equals(value)) {
        result = 1 + i
        flag = false
      }
    }
    result
  }

  /**
   * 合并所有特征
   * @param values
   * @param allVocabSizeMap
   * @return
   */
  def combineAllFeature(values: Map[String, Int], allVocabSizeMap: Map[String, Int]): org.apache.spark.ml.linalg.Vector = {
    var prefix = 0
    val features = ArrayBuffer[(Int, Double)]()
    for (x <- denseFeatureColumnName.++(sparseFeatureColumnName)) {
      features.append(
        (prefix + values(x), 1.0)
      )
      prefix += allVocabSizeMap(x)
    }
    if (Random.nextInt(1000) == 0) {
      println(values, features)
    }
    org.apache.spark.ml.linalg.Vectors.sparse(allVocabSizeMap.values.sum, features)
  }

}
