package org.datacenter.config.receiver.base;

import lombok.Data;

/**
 * @author : [wangminan]
 * @description : 基础配置
 */
@Data
public abstract class BaseReceiverConfig {
    protected DataSourceType dataSourceType;
}
