package com.github.tangjun5555.recsys.spark.`match`

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * author: tangj
 * time: 2020/1/15 14:21
 * description: 基于标签偏好的推荐召回
 *
 * 应用场景:
 * 1、信息流（资讯、短视频）推荐中关于主题、作者、关键词标签的推荐召回
 * 2、电商购物推荐中关于类目的推荐召回
 * 3、广告推荐中关于素材类别（游戏、教育等）的推荐召回
 *
 * 时间衰减函数
 * a = math.exp(-0.1 * d) d为距离今天的天数
 * w = a * a
 */
class TagPreference extends U2IMatch {

  private var spark: SparkSession = _

  private var dataDF: DataFrame = _

  private var userColumnName: String = "user"

  def setUserColumnName(value: String): this.type = {
    this.userColumnName = value
    this
  }

  def getUserColumnName(): String = {
    this.userColumnName
  }

  private var itemColumnName: String = "item"

  def setItemColumnName(value: String): this.type = {
    this.itemColumnName = value
    this
  }

  def getItemColumnName(): String = {
    this.itemColumnName
  }

  override def recommendForUser(recNum: Int, withScore: Boolean, recResultColumnName: String): DataFrame = ???

}
