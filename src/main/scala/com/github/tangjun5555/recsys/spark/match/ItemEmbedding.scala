package com.github.tangjun5555.recsys.spark.`match`

import org.apache.spark.sql.DataFrame

/**
 * author: tangj 1844250138@qq.com
 * time: 2020/2/17 15:42
 * description: 物品Embedding
 */
trait ItemEmbedding extends Serializable {

  def getItemEmbedding(
                        vectorAsString: Boolean = false
                      ): DataFrame

}
