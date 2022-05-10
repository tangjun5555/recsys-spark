package com.github.tangjun5555.recsys.spark.jutil;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
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
    public static DateTimeFormatter formatter3 = DateTimeFormatter.ofPattern("yyyyMMdd");

    public static String getCurrentDt() {
        return LocalDate.now().format(formatter3);
    }

    /**
     * 计算两个时间点相隔的秒数
     * @param start
     * @param end
     * @return
     */
    public static Integer computeDiffSeconds(String start, String end) {
        LocalDateTime t1 = LocalDateTime.parse(start, formatter2);
        LocalDateTime t2 = LocalDateTime.parse(end, formatter2);
        return Integer.parseInt(ChronoUnit.SECONDS.between(t1, t2) + "");
    }

    /**
     * 获取当前时间点
     * @return
     */
    public static String getCurrentDateTime() {
        return LocalDateTime.now().format(formatter2);
    }

    /**
     * 计算相隔period日期
     * @param start
     * @param period
     * @return
     */
    public static String computeDiffDate(String start, int period) {
        return LocalDate.parse(start, formatter3).plusDays(period).format(formatter3);
    }

    /**
     * 将毫秒级别的timestamp转为LocalDateTime
     * @param timestamp
     * @return
     */
    public static String getDateTimeOfTimestamp(long timestamp) {
        Instant instant = Instant.ofEpochMilli(timestamp);
        ZoneId zone = ZoneId.systemDefault();
        return LocalDateTime.ofInstant(instant, zone).format(formatter2);
    }

}
