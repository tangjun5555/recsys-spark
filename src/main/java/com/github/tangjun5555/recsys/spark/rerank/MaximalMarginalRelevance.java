package com.github.tangjun5555.recsys.spark.rerank;

import java.util.*;

/**
 * author: tangj 1844250138@qq.com
 * time: 2021/10/27 4:18 下午
 * description: MMR
 */
public class MaximalMarginalRelevance extends Diversity {

    private final double lambda;

    public MaximalMarginalRelevance(double lambda) {
        this.lambda = lambda;
    }

    @Override
    List<String> select(int n, String[] itemIds, double[] qualityScores, double[][] similarityMatrix) {
        List<String> result = new ArrayList<>();

        int itemSize = itemIds.length;
        Map<String, Double> qualityScoresMap = new HashMap<>(itemSize);
        Map<String, Integer> itemIndexMap = new HashMap<>(itemSize);
        for (int i = 1; i < itemSize; i++) {
            qualityScoresMap.put(itemIds[i], qualityScores[i]);
            itemIndexMap.put(itemIds[i], i);
        }
        List<String> reservedItemIds = new ArrayList<>(Arrays.asList(itemIds));

        String selectItem = itemIds[0];
        double selectItemScore = qualityScores[0];
        for (int i = 1; i < itemSize; i++) {
            if (qualityScores[i] > selectItemScore) {
                selectItem = itemIds[i];
                selectItemScore = qualityScores[i];
            }
        }
        result.add(selectItem);
        reservedItemIds.remove(selectItem);

        while (result.size() < n && result.size() < itemSize) {
            Double mmrScore = null;
            // 遍历未被选择物品
            for (String i : reservedItemIds) {
                String maxSimItem = result.get(0);
                double maxSimItemDegree = similarityMatrix[itemIndexMap.get(i)][itemIndexMap.get(maxSimItem)];
                // 遍历已被选择物品
                for (String j : result.subList(1, result.size())) {
                    double tmpSimItemDegree = similarityMatrix[itemIndexMap.get(i)][itemIndexMap.get(j)];
                    if (tmpSimItemDegree > maxSimItemDegree) {
                        maxSimItem = j;
                        maxSimItemDegree = tmpSimItemDegree;
                    }
                }
                System.out.println("sim " + i + "&" + maxSimItem + ":");
                double mmrScoreTmp =  lambda * (qualityScoresMap.get(i) - (1 - lambda) * maxSimItemDegree);
                if (Objects.isNull(mmrScore)) {
                    mmrScore = mmrScoreTmp;
                    selectItem = i;
                } else {
                    if (mmrScoreTmp > mmrScore) {
                        mmrScore = mmrScoreTmp;
                        selectItem = i;
                    }
                }
            }
            result.add(selectItem);
            reservedItemIds.remove(selectItem);
        }

        return result;
    }

}
