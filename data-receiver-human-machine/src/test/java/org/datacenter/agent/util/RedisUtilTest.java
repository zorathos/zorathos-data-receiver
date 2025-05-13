package org.datacenter.agent.util;

import lombok.extern.slf4j.Slf4j;
import org.datacenter.config.HumanMachineConfig;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

/**
 * @author : [wangminan]
 * @description : {@link RedisUtil}测试
 */
@Slf4j
public class RedisUtilTest {
    public static void main(String[] args) {
        log.info("ZoneId: {}", ZoneId.of("Asia/Shanghai"));
        // 毫秒级时间戳转localdatetime 1747065600000
        long timestamp = 1747068800000L;
        LocalDateTime localDateTime = LocalDateTime.ofInstant(
                Instant.ofEpochMilli(timestamp),
                ZoneId.of("Asia/Shanghai"));
        log.info("localDateTime: {}", localDateTime);
        HumanMachineConfig config = new HumanMachineConfig();
        config.loadConfig();
        RedisUtil.initPool();
        boolean del = RedisUtil.del("sorties:ATC-3程_辛_鸿陈_泽彦+ATCs青_林_鹏辉");
        System.out.println(del);
    }
}
