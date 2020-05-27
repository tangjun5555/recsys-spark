package org.apache.spark.ml.lsh

import org.apache.spark.sql.DataFrame

/**
 * author: tangj
 * time: 2020/5/21 10:29
 * description: 基于LSH实现內积最大检索
 */
class InnerProductANNSearch {

  private var queryIdColumnName: String = "query_id"

  private var queryVectorColumnName: String = "query_vector"

  private var docIdColumnName: String = "doc_id"

  private var docVectorColumnName: String = "doc_vector"

  def fit(docVectorDF: DataFrame): this.type = {
    this
  }

  def search(queryVectorDF: DataFrame): this.type = {
    this
  }

}
