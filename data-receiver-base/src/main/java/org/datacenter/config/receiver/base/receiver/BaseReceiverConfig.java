package org.datacenter.config.receiver.base.receiver;

import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * @author : [wangminan]
 * @description : 基础配置
 */
@Data
public abstract class BaseReceiverConfig {
    protected DataSourceType dataSourceType;
}
