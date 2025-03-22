package org.datacenter.receiver.util;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;

/**
 * @author : [wangminan]
 * @description : java时间转换为SQL时间工具类
 */
public class JavaTimeUtil {
    /**
     * 将LocalTime转换为Unix时间戳
     *
     * @param time 消息时间
     * @return Unix时间戳
     */
    public static long convertLocalTimeToUnixTimestamp(LocalDate date, LocalTime time) {
        // Combine date and time
        LocalDateTime dateTime = LocalDateTime.of(date, time);

        // Convert to Instant
        Instant instant = dateTime.toInstant(ZoneOffset.UTC);

        // Get Unix timestamp
        return instant.getEpochSecond();
    }
}
