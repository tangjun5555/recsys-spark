package com.github.tangjun5555.recsys.spark.`match`

import org.apache.spark.sql.DataFrame

/**
 * author: tangj 1844250138@qq.com
 * time: 2021/3/31 5:34 下午
 * description:
 */
class ExpectationI2I extends I2IMatch {

  override def recommendForItem(recNum: Int, withScore: Boolean, recResultColumnName: String): DataFrame = {
    null
  }

}
