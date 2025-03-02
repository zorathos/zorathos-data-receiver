package org.datacenter.config.basic;

/**
 * @author : [wangminan]
 * @description : 数据接收器基类
 */
public abstract class BaseReceiver {

    protected BaseConfig config;

    public abstract void prepare();

    public abstract void start();

    public abstract void stop();
}
