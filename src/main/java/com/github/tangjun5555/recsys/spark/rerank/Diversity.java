package com.github.tangjun5555.recsys.spark.rerank;

import java.util.List;

/**
 * author: tangj 1844250138@qq.com
 * time: 2021/10/27 4:19 下午
 * description: 平衡相关性与多样性
 */
public abstract class Diversity {

    abstract List<String> select(int n, String[] itemIds, double[] qualityScores, double[][] similarityMatrix);

}
