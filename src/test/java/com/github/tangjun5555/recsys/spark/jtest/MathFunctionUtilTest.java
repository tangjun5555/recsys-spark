package com.github.tangjun5555.recsys.spark.jtest;

import com.github.tangjun5555.recsys.spark.jutil.MathFunctionUtil;
import org.junit.Test;

/**
 * author: tangj 1844250138@qq.com
 * time: 2020/6/8 2:54 下午
 * description:
 */
public class MathFunctionUtilTest {

    @Test
    public void test2() {
        System.out.println(MathFunctionUtil.walsonCtr(1.0, 10.0, 1.96));
        System.out.println(MathFunctionUtil.walsonCtr(10.0, 100.0, 1.96));
        System.out.println(MathFunctionUtil.walsonCtr(100.0, 1000.0, 1.96));

        System.out.println(MathFunctionUtil.walsonCtr(1, 1, 1.96));
        System.out.println(MathFunctionUtil.walsonCtr(10, 10, 1.96));
        System.out.println(MathFunctionUtil.walsonCtr(100, 100, 1.96));
        System.out.println(MathFunctionUtil.walsonCtr(2000, 1000, 1.96));
    }

    @Test
    public void test() {
        System.out.println(MathFunctionUtil.binaryLogLoss(0.0, 0.001));
        System.out.println(MathFunctionUtil.binaryLogLoss(0.0, 0.7));
        System.out.println(MathFunctionUtil.binaryLogLoss(0.0, 0.6));
        System.out.println(MathFunctionUtil.binaryLogLoss(0.0, 0.999));

        System.out.println(MathFunctionUtil.binaryLogLoss(0.0, 1.0));
        System.out.println(MathFunctionUtil.binaryLogLoss(0.0, 0.0));
        System.out.println(MathFunctionUtil.binaryLogLoss(0.0, 0.0) == 0.0);
    }

}
