package org.datacenter.receiver;

import lombok.extern.slf4j.Slf4j;
import org.datacenter.agent.BaseAgent;
import org.datacenter.config.HumanMachineConfig;
import org.datacenter.config.BaseReceiverAndAgentConfig;
import org.datacenter.exception.ZorathosException;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;

/**
 * @author : [wangminan]
 * @description : 数据接收器基类
 */
@Slf4j
public abstract class BaseReceiver {

    protected BaseReceiverAndAgentConfig receiverConfig;

    public void run() {
        prepare();
        start();
    }

    public void prepare() {
        HumanMachineConfig humanMachineSysConfig = new HumanMachineConfig();
        humanMachineSysConfig.loadConfig();
    }

    public abstract void start();

    /**
     * 等待agent启动完成 给所有需要启动的agent的receiver使用
     *
     * @param baseAgent agent
     */
    protected void awaitAgentRunning(BaseAgent baseAgent) {
        log.info("Waiting for agent to get in running status...");
        // 阻塞 直到Agent切换到running=true的状态 尽量用锁或者COUNTDOWNLATCH 不要在while里面加sleep
        CountDownLatch latch = new CountDownLatch(1);
        Timer timer = new Timer();
        // 每秒检查一次agent是否已经启动
        new Thread(() -> timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                if (baseAgent.isRunning()) {
                    latch.countDown();
                }
            }
        }, 0, 1000)).start();

        try {
            log.info("Main thread acknowledges that agent is started. Free up the latch.");
            latch.await();
            timer.cancel();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ZorathosException(e, "Thread was interrupted while waiting for personnel agent to be ready.");
        }
    }

    /**
     * 封装的agent停止方法
     * 由于agent一般使用了更深层的定时线程池 所以这玩意仅限能够捕获prepare阶段的异常 run阶段的异常要通过exit 1的方式来出发最外层的shutdownhook
     * @param agent agent
     */
    protected void agentShutdown(BaseAgent agent) {
        try {
            log.info("Agent shutting down. It could happened both in normal and abnormal cases.");
            agent.stop();
        } catch (Exception e) {
            throw new ZorathosException(e, "Encounter error when stopping agent. You may need to check redis to delete the key manually.");
        }
    }
}
