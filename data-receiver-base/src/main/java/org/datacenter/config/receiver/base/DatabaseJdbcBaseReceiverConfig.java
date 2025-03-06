package org.datacenter.config.receiver.base;

import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * @author : [wangminan]
 * @description : 数据库JDBC接收器配置
 */
@Data
@EqualsAndHashCode(callSuper = true)
public abstract class DatabaseJdbcBaseReceiverConfig extends BaseReceiverConfig {
    protected DataSourceType dataSourceType = DataSourceType.DATABASE_JDBC;
}
