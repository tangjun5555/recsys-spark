package com.github.tangjun5555.recsys.spark.util

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * author: tangjun 1844250138@qq.com
 * time: 2018/8/5 20:21
 * description:
 */
object SparkUtil {

  def getSparkSession(name: String, cores: Int = 3, logLevel: Level = Level.INFO, driverMemory: String = "2g"): SparkSession = {
    if (System.getProperties.getProperty("os.name").contains("Windows")
      || System.getProperties.getProperty("os.name").contains("Mac OS")) {
      getLocalSparkSession(name, logLevel, cores, driverMemory)
    } else {
      getClusterSparkSession(name, logLevel)
    }
  }

  def getLocalSparkSession(name: String, logLevel: Level = Level.INFO, cores: Int = 3, driverMemory: String = "2g"): SparkSession = {
    Logger.getLogger("org.apache.spark").setLevel(logLevel)
    System.setProperty("hadoop.home.dir", System.getenv("HADOOP_HOME"))
    SparkSession
      .builder()
      .appName(name)
      .master(s"local[${cores}]")
      .config("spark.sql.warehouse.dir", "warehouse")
      .config("spark.driver.memory", driverMemory)
      .getOrCreate()
  }

  def getClusterSparkSession(name: String, logLevel: Level = Level.INFO): SparkSession = {
    Logger.getLogger("org.apache.spark").setLevel(logLevel)
    SparkSession
      .builder()
      .appName(name)
      .enableHiveSupport()
      .getOrCreate()
  }

  /**
   * 将DataFrame保存为TFRecords格式
   * @param df
   * @param path
   */
  def writeDF2TFRecords(df: DataFrame, path: String): Unit = {
    df.write
      .format("tfrecords")
      .mode("overwrite")
      .option("recordType", "Example")
      .save(path)
  }

  /**
   * TODO
   * 将pipeline保存为pmml格式
   * @param schema
   * @param pipeline
   * @param path
   */
  def pipeline2PMML(schema: StructType, pipeline: Pipeline, path: String): Unit = {

  }

}
