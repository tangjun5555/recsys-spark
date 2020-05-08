package indi.tangjun.recsys.spark.`match`

import org.apache.spark.sql.DataFrame

/**
 * author: tangj
 * time: 2020/5/8 13:16
 * description:
 */
class SimRank extends I2IMatch {
  override def recommendForItem(recNum: Int, withScore: Boolean, recResultColumnName: String): DataFrame = ???
}
