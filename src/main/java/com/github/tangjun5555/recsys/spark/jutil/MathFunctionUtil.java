package com.github.tangjun5555.recsys.spark.jutil;

import org.apache.commons.collections.CollectionUtils;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author: tangjun 1844250138@qq.com
 * time: 2020/1/9 18:43
 * description:
 */
public class MathFunctionUtil {

    public static double walsonCtr(double click, double expo, double z) {
        assert (click >= 0.0);
        assert (expo > 0.0);

        double p = click / expo;
        if (p > 1.0) {
            p = 1.0;
        }

        double t0 = Math.pow(z, 2);
        double t1 = 1.0 + t0 / expo;
        double t2 = p + t0 / (2 * expo);
        double t3 = Math.sqrt(p * (1.0 - p) / expo + t0 / (4 * Math.pow(expo, 2)));

        p = t2 / t1 - z / t1 * t3;

        return p;
    }

    /**
     * @param label
     * @param probability
     * @return
     */
    public static double binaryLogLoss(double label, double probability) {
        assert Arrays.asList(0.0, 1.0).contains(label);
        assert probability >= 0.0 && probability <= 1.0;
        double v = 1e-7;

        if (label == 1.0) {
            if (probability == 0.0) {
                return -Math.log(v);
            }
            return -Math.log(probability);
        } else {
            if (probability == 1.0) {
                return -Math.log(v);
            }
            return -Math.log(1 - probability);
        }
    }

    public static double sigmoid(double value) {
        return 1.0 / (1.0 + Math.exp(-value));
    }

    /**
     * 计算Recall@N
     *
     * @param labels   不重复元素组合
     * @param predicts 不重复元素组合
     * @param num
     * @return
     */
    public static double computeRecRecallRate(List<String> labels, List<String> predicts, int num) {
        assert !CollectionUtils.isEmpty(labels);
        assert num > 0;

        double result = 0.0;
        if (!CollectionUtils.isEmpty(predicts)) {
            Set<String> validItems = new HashSet<>();
            if (predicts.size() >= num) {
                predicts = predicts.subList(0, num);
            }
            for (String item : predicts) {
                if (labels.contains(item)) {
                    validItems.add(item);
                }
            }
            result = (1.0 * validItems.size()) / labels.size();
        }
        return result;
    }

}
