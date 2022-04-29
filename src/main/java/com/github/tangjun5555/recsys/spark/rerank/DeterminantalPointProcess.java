package com.github.tangjun5555.recsys.spark.rerank;

import java.util.ArrayList;
import java.util.List;

/**
 * @author: tangj 1844250138@qq.com
 * time: 2021/10/26 4:58 下午
 * description: DPP
 */
public class DeterminantalPointProcess extends Diversity {

    @Override
    List<String> select(int n, String[] itemIds, double[] qualityScores, double[][] similarityMatrix) {
        List<String> result = new ArrayList<>();
        int itemSize = itemIds.length;

        // TODO

        return result;
    }

}
