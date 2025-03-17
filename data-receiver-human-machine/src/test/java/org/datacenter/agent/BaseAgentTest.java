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

    private static final TestAgent1 testAgent1;
    private static final TestAgent2 testAgent2;
    private static final TestAgent1 testAgent3;

    static {
        HumanMachineSysConfig humanMachineSysConfig = new HumanMachineSysConfig();
        humanMachineSysConfig.loadConfig();
        testAgent1 = new TestAgent1();
        testAgent2 = new TestAgent2();
        testAgent3 = new TestAgent1();
    }

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
        testAgent1.run();
        testAgent2.run();
        testAgent3.run();
        log.info("Agent1 running:{}", testAgent1.prepared);
        log.info("Agent2 running:{}", testAgent2.prepared);
        log.info("Agent3 running:{}", testAgent3.prepared);
    }

    @Test
    void testStop() {
        HumanMachineSysConfig humanMachineSysConfig = new HumanMachineSysConfig();
        humanMachineSysConfig.loadConfig();
        testAgent1.stop();
        testAgent2.stop();
        testAgent3.stop();
    }
}
