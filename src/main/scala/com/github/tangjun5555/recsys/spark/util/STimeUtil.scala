package com.github.tangjun5555.recsys.spark.util

import java.time.{Instant, LocalDate, LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter

/**
 * author: tangj 1844250138@qq.com
 * time: 2020/6/17 3:19 下午
 * description:
 */
object STimeUtil {

  def getCurrentDate(formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")): String = {
    LocalDate.now().format(formatter)
  }

  def computeDiffDate(targetDate: String, period: Int, formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")): String = {
    LocalDate.parse(targetDate, formatter)
      .plusDays(period)
      .format(formatter)
  }

  def timestamp2DateTime(value: Long, formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")): String = {
    formatter.format(LocalDateTime.ofInstant(Instant.ofEpochMilli(value), ZoneId.systemDefault()))
  }

  def main(args: Array[String]): Unit = {
    println(getCurrentDate())

    println(computeDiffDate(getCurrentDate(), -10))

    println(timestamp2DateTime(1592241927766L))
  }

}
