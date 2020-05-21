package com.github.tangjun5555.recsys.spark.util

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
  * author: tangjun tangj@2345.com
  * time: 2019/6/10 18:05
  * description:
  */
object ConcatIdByRank extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = StructType(Array(
    StructField("id", StringType),
    StructField("rank", IntegerType)
  ))

  override def bufferSchema: StructType = StructType(Array(
    StructField("buffer", StringType)
  ))

  override def dataType: DataType = StringType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = ""
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getString(0) + "," + (input.getString(0) + "#" + input.getInt(1))
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getString(0) + "," + buffer2.getString(0)
  }

  override def evaluate(buffer: Row): Any = {
    buffer.getString(0).split(",")
      .filter(!"".equals(_))
      .map(x => {
        val split = x.split("#")
        (split(0), split(1).toInt)
      })
      .sortBy(_._2)
      .map(_._1)
      .mkString(",")
  }

}
