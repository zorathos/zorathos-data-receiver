package org.datacenter.agent;

import lombok.extern.slf4j.Slf4j;
import org.datacenter.config.system.HumanMachineSysConfig;
import org.junit.jupiter.api.Test;

/**
 * @author : [wangminan]
 * @description : 测试BaseAgent
 */
@Slf4j
public class BaseAgentTest {
    private static class TestAgent1 extends BaseAgent {
        public TestAgent1() {
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

    private static class TestAgent2 extends BaseAgent {
        public TestAgent2() {
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
        TestAgent1 testAgent1 = new TestAgent1();
        testAgent1.run();
        TestAgent1 testAgent2 = new TestAgent1();
        testAgent2.run();
        log.info("Agent1 running:{}", testAgent1.running);
        log.info("Agent2 running:{}", testAgent2.running);
    }

    @Test
    void testStop() {
        HumanMachineSysConfig humanMachineSysConfig = new HumanMachineSysConfig();
        humanMachineSysConfig.loadConfig();
        TestAgent1 testAgent1 = new TestAgent1();
        testAgent1.stop();
    }
}
