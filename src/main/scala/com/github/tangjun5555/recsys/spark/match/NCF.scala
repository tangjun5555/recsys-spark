package com.github.tangjun5555.recsys.spark.`match`
import org.apache.spark.sql.DataFrame

/**
 * author: tangj
 * time: 2020/5/16 18:38
 * description:
 */
class NCF extends UserEmbedding with ItemEmbedding {
  override def getUserEmbedding(vectorAsString: Boolean): DataFrame = ???

  override def getItemEmbedding(vectorAsString: Boolean): DataFrame = ???
}
