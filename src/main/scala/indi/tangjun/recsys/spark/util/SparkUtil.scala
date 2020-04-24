package indi.tangjun.recsys.spark.util

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
 * author: tangjun 1844250138@qq.com
 * time: 2018/8/5 20:21
 * description:
 */
object SparkUtil {

  def getSparkSession(name: String, cores: Int = 3, logLevel: Level = Level.INFO): SparkSession = {
    if (System.getProperties.getProperty("os.name").contains("Windows")) {
      getLocalSparkSession(name, cores, logLevel)
    } else {
      getClusterSparkSession(name, logLevel)
    }
  }

  def getLocalSparkSession(name: String, cores: Int = 3, logLevel: Level = Level.INFO): SparkSession = {
    Logger.getLogger("org.apache.spark").setLevel(logLevel)
    System.setProperty("hadoop.home.dir", System.getenv("hadoop_home"))
    SparkSession
      .builder()
      .appName(name)
      .master(s"local[${cores}]")
      .config("spark.sql.warehouse.dir", "warehouse")
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

}
