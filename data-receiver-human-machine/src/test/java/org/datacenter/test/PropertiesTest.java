package org.datacenter.test;

import org.datacenter.config.HumanMachineConfig;
import org.junit.jupiter.api.Test;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;

/**
 * @author : [wangminan]
 * @description : 测试配置的属性
 */
public class PropertiesTest {

    @Test
    void testRead() {
        HumanMachineConfig humanMachineSysConfig = new HumanMachineConfig();
        humanMachineSysConfig.loadConfig();
    }

    @Test
    void testStore() {
        Properties properties = new Properties();
        properties.setProperty("agent.interval.sorties", "10");
        properties.setProperty("agent.interval.sorties.batch", "10");
        // 写
        try (FileWriter fileWriter = new FileWriter("D:\\test.properties")) {
            properties.store(fileWriter, "test");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
