package com.github.tangjun5555.recsys.spark.`match`

import org.apache.spark.sql.DataFrame

/**
 * author: tangj 1844250138@qq.com
 * time: 2020/2/16 12:18
 * description: I2I召回接口
 */
trait I2IMatch extends Serializable {

  def recommendForItem(
                        recNum: Int = 50
                        , withScore: Boolean = false
                        , recResultColumnName: String = "rec_items"
                      ): DataFrame

}
