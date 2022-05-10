package com.github.tangjun5555.recsys.spark.param;

/**
 * author: tangj 1844250138@qq.com
 * time: 2021/4/23 12:19 下午
 * description:
 */
public enum TaskTypeEnum {

    /**
     * 预测
     */
    PREDICT("predict"),

    /**
     * 评估
     */
    TEST("test"),

    /**
     * 训练
     */
    TRAIN("train");

    private String value;

    TaskTypeEnum(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return value;
    }

}
