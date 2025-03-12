package org.datacenter.agent;

import lombok.extern.slf4j.Slf4j;
import org.datacenter.config.system.HumanMachineSysConfig;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;

/**
 * @author : [wangminan]
 * @description : 测试BaseAgent
 */
@Slf4j
public class BaseAgentTest {
    private static class TestAgent extends BaseAgent {
        public TestAgent() {
            super();
        }

        @Override
        public void run() {
            super.run();
        }

        @Override
        public void stop() {
            super.stop();
        }
    }

    @Test
    void testRun() {
        HumanMachineSysConfig humanMachineSysConfig = new HumanMachineSysConfig();
        humanMachineSysConfig.loadConfig();
        TestAgent testAgent = new TestAgent();
        testAgent.run();
        log.info("Agent running:{}", testAgent.running);
    }

    @Test
    void testStop() {
        HumanMachineSysConfig humanMachineSysConfig = new HumanMachineSysConfig();
        humanMachineSysConfig.loadConfig();
        TestAgent testAgent = new TestAgent();
        testAgent.stop();
    }
}
