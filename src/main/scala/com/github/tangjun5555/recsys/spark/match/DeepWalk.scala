package com.github.tangjun5555.recsys.spark.`match`

import org.apache.spark.sql.DataFrame

/**
 * author: tangj 1844250138@qq.com
 * time: 2020/4/11 12:04
 * description:
 *
 * 算法流程
 * 1、收集用户行为序列
 * 2、构建物品关系图
 * 3、随机游走产生物品序列
 * 4、通过word2vec产生物品向量
 *
 */
class DeepWalk extends ItemEmbedding {
  override def getItemEmbedding(vectorAsString: Boolean): DataFrame = ???
}
