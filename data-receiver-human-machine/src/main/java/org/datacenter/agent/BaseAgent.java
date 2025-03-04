package org.datacenter.agent;

/**
 * Agent只出现在人际能力匹配验证系统中 用于从老项目接口中拉取数据并投递
 * @author : [wangminan]
 * @description : 基础的Agent，如有需要 将在Receiver的Prepare阶段被调用
 */
public abstract class BaseAgent implements Runnable {
    protected volatile boolean running = true;

    @Override
    public void run() {
    }

    public void stop() {
        running = false;
    }
}
