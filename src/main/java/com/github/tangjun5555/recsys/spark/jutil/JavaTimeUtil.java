package com.github.tangjun5555.recsys.spark.jutil;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

/**
 * author: tangj
 * time: 2019/6/11 19:37
 * description:
 */
public class JavaTimeUtil {

    public static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    public static DateTimeFormatter formatter2 = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static Integer computeDiffSeconds(String start, String end) {
        LocalDateTime t1 = LocalDateTime.parse(start, formatter2);
        LocalDateTime t2 = LocalDateTime.parse(end, formatter2);
        return Integer.parseInt(ChronoUnit.SECONDS.between(t1, t2) + "");
    }

}
