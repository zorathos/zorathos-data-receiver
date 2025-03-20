package org.datacenter.receiver;

import lombok.extern.slf4j.Slf4j;
import org.datacenter.agent.BaseAgent;
import org.datacenter.config.BaseReceiverConfig;
import org.datacenter.config.system.HumanMachineSysConfig;
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

    protected BaseReceiverConfig receiverConfig;

    public void run() {
        prepare();
        start();
    }

    public void prepare() {
        HumanMachineSysConfig humanMachineSysConfig = new HumanMachineSysConfig();
        humanMachineSysConfig.loadConfig();
    }

    public abstract void start();

    /**
     * 等待agent启动完成 给所有需要启动的agent的receiver使用
     *
     * @param baseAgent agent
     */
    protected void awaitAgentRunning(BaseAgent baseAgent) {
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
            latch.await();
            timer.cancel();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ZorathosException(e, "Thread was interrupted while waiting for personnel agent to be ready.");
        }
    }
}
