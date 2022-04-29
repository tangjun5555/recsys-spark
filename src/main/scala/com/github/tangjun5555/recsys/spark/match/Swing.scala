package com.github.tangjun5555.recsys.spark.`match`
import org.apache.spark.sql.DataFrame

/**
 * author: tangj 1844250138@qq.com
 * time: 2021/11/3 2:55 下午
 * description:
 */
class Swing extends I2IMatch {
  override def recommendForItem(recNum: Int, withScore: Boolean, recResultColumnName: String): DataFrame = ???
}
