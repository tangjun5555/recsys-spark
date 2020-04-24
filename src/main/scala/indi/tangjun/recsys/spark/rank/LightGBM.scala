package indi.tangjun.recsys.spark.rank

import com.microsoft.ml.spark.lightgbm.LightGBMClassifier
import org.apache.spark.sql.DataFrame

/**
 * author: tangj
 * time: 2020/4/24 18:01
 * description:
 */
class LightGBM extends LightGBMClassifier {

  def fit(dataDF: DataFrame): this.type = {

    this
  }

  def predict(dataDF: DataFrame): DataFrame = {
    dataDF
  }

}
