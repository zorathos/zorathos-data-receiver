package org.datacenter.agent.util;

import lombok.extern.slf4j.Slf4j;
import org.datacenter.config.HumanMachineConfig;

import java.time.ZoneId;

/**
 * @author : [wangminan]
 * @description : {@link RedisUtil}测试
 */
@Slf4j
public class RedisUtilTest {
    public static void main(String[] args) {
        log.info("ZoneId: {}", ZoneId.of("Asia/Shanghai"));
        HumanMachineConfig config = new HumanMachineConfig();
        config.loadConfig();
        RedisUtil.initPool();
        boolean del = RedisUtil.del("sorties:ATC-3程_辛_鸿陈_泽彦+ATCs青_林_鹏辉");
        System.out.println(del);
    }
}
