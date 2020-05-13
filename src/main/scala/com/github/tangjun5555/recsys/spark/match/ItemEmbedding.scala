package com.github.tangjun5555.recsys.spark.`match`

import org.apache.spark.sql.DataFrame

/**
 * author: tangj
 * time: 2020/2/17 15:42
 * description:
 */
trait ItemEmbedding extends Serializable {

  def getItemEmbedding(
                        vectorAsString: Boolean = false
                      ): DataFrame

}
