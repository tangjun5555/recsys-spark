package com.github.tangjun5555.recsys.spark.jtest;

import com.github.tangjun5555.recsys.spark.jutil.JavaTimeUtil;
import org.junit.Test;

/**
 * author: tangj 1844250138@qq.com
 * time: 2020/6/4 3:11 下午
 * description:
 */
public class JavaTimeUtilTest {

    @Test
    public void test() {
        String result = JavaTimeUtil.computeDiffDate("20200602", -14);
        System.out.println(result);
    }

}
