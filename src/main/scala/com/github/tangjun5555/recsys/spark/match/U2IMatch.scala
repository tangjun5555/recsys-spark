package com.github.tangjun5555.recsys.spark.`match`

import org.apache.spark.sql.DataFrame

/**
 * author: tangj
 * time: 2020/2/16 12:19
 * description: U2I召回接口
 */
trait U2IMatch extends Serializable {

  def recommendForUser(
                        recNum: Int = 50
                        , withScore: Boolean = false
                        , recResultColumnName: String = "rec_items"
                      ): DataFrame

}
