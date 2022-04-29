package com.github.tangjun5555.recsys.spark.inference;

import ml.dmlc.xgboost4j.LabeledPoint;
import ml.dmlc.xgboost4j.java.Booster;
import ml.dmlc.xgboost4j.java.XGBoost;

import java.util.List;

/**
 * author: tangj 1844250138@qq.com
 * time: 2022/4/29 16:31
 * description:
 */
public class XGBoostModel extends RankModel {

    private Booster booster;

    public XGBoostModel(String modelFile) throws Exception {
        this.booster = XGBoost.loadModel(modelFile);
    }

    public List<Double> predict(List<String> modelFeatures) {
        return null;
    }

    private List<Double> inference(List<LabeledPoint> labeledPoints) {
        return null;
    }

}
