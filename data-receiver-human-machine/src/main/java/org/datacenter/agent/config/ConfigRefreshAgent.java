package org.datacenter.agent.config;

import lombok.extern.slf4j.Slf4j;

/**
 * @author : [wangminan]
 * @description : 预留的配置热更新Agent 他和BaseAgent这样的接收Agent没有关系 每个实例都需要带上这个Agent
 */
@Slf4j
public class ConfigRefreshAgent implements Runnable{

    @Override
    public void run() {
        throw new UnsupportedOperationException("ConfigRefreshAgent is not implemented yet.");
    }
}
