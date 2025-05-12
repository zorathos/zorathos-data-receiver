package org.datacenter.agent.util;

import org.datacenter.config.HumanMachineConfig;

/**
 * @author : [wangminan]
 * @description : {@link RedisUtil}测试
 */
public class RedisUtilTest {
    public static void main(String[] args) {
        HumanMachineConfig config = new HumanMachineConfig();
        config.loadConfig();
        RedisUtil.initPool();
        boolean del = RedisUtil.del("sorties:ATC-3程_辛_鸿陈_泽彦+ATCs青_林_鹏辉");
        System.out.println(del);
    }
}
