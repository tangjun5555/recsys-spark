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
    public void test3() {
        long t = 1595378651966L;
        System.out.println("结果:" + JavaTimeUtil.getDateTimeOfTimestamp(t));
    }

    @Test
    public void test2() {
        String start = "2018-04-12 13:58:02";
        String end = "2018-04-13 13:58:02";
        System.out.println(JavaTimeUtil.computeDiffSeconds(start, end));
    }

    @Test
    public void test() {
        String result = JavaTimeUtil.computeDiffDate("20200602", -14);
        System.out.println(result);
    }

}
