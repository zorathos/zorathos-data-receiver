package org.datacenter.agent;

/**
 * @author : [wangminan]
 * @description : 基础的Agent，如有需要 将在Receiver的Prepare阶段被调用
 */
public abstract class BaseAgent implements Runnable{
    protected volatile boolean running = true;

    @Override
    public void run() {
    }

    private void stop() {
        running = false;
    }
}
