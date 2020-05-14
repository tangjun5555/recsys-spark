package com.github.tangjun5555.recsys.spark.jutil;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

/**
 * author: tangjun
 * time: 2019/6/11 19:37
 * description:
 */
public class JavaTimeUtil {

    public static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    public static DateTimeFormatter formatter2 = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static String getTodayDate() {
        return LocalDate.now().format(formatter);
    }

    public static Integer computeDiffDays(String start, String end) {
        LocalDate t1 = LocalDate.parse(start, formatter2);
        LocalDate t2 = LocalDate.parse(end, formatter);
        return Integer.parseInt(ChronoUnit.DAYS.between(t1, t2) + "");
    }

    public static Integer computeCreatedDays(String createTime) {
        return computeDiffDays(createTime, getTodayDate());
    }

    public static Integer computeDiffSeconds(String start, String end) {
        LocalDateTime t1 = LocalDateTime.parse(start, formatter2);
        LocalDateTime t2 = LocalDateTime.parse(end, formatter2);
        return Integer.parseInt(ChronoUnit.SECONDS.between(t1, t2) + "");
    }

    public static Boolean isValidFormatter(String value, DateTimeFormatter formatter) {
        boolean result = true;
        try {
            LocalDate.parse(value, formatter);
        } catch (Exception e) {
            result = false;
        }
        return result;
    }

    public static String computeDayOfWeek(String date) {
        LocalDate parse = LocalDate.parse(date, formatter);
        return parse.getDayOfWeek().toString();
    }

    public static String computeDiffDate(String date, int period) {
        return LocalDate.parse(date, formatter).plusDays(period).format(formatter);
    }

}
