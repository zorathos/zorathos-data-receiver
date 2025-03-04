package org.datacenter.receiver;

import lombok.extern.slf4j.Slf4j;
import org.datacenter.config.receiver.base.receiver.BaseReceiverConfig;

/**
 * @author : [wangminan]
 * @description : 数据接收器基类
 */
@Slf4j
public abstract class BaseReceiver {

    protected BaseReceiverConfig config;

    public void run() {
        prepare();
        start();
    }

    public abstract void prepare();

    public abstract void start();
}
