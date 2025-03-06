package org.datacenter.config.receiver.base;

import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * @author : [wangminan]
 * @description : 数据库配置 使用Flink CDC
 */
@Data
@EqualsAndHashCode(callSuper = true)
public abstract class DatabaseCdcBaseReceiverConfig extends BaseReceiverConfig {
    protected DataSourceType dataSourceType = DataSourceType.DATABASE_CDC;
}
