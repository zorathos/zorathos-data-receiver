package org.datacenter.config.receiver.base;

import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * @author : [wangminan]
 * @description : 文件接收器基础配置
 */
@Data
@EqualsAndHashCode(callSuper = true)
public abstract class FileBaseReceiverConfig extends BaseReceiverConfig {
    protected DataSourceType dataSourceType = DataSourceType.FILE;
}
