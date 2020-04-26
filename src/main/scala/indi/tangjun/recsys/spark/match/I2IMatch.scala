package indi.tangjun.recsys.spark.`match`

import org.apache.spark.sql.DataFrame

/**
 * author: tangj
 * time: 2020/2/16 12:18
 * description:
 */
trait I2IMatch extends Serializable {

  def recommendForItem(
                        recNum: Int = 50
                        , withScore: Boolean = false
                        , recResultColumnName: String = "rec_items"
                      ): DataFrame

}
