package indi.tangjun.recsys.spark.jutil;

import org.apache.commons.collections.CollectionUtils;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author: tangjun
 * time: 2020/1/9 18:43
 * description:
 */
public class MathFunctionUtil {

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

    /**
     * 计算Precision@N
     *
     * @param labels   不重复元素组合
     * @param predicts 不重复元素组合
     * @param num
     * @return
     */
    public static double computeRecPrecisionRate(List<String> labels, List<String> predicts, int num) {
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
            result = (1.0 * validItems.size()) / num;
        }
        return result;
    }

}
