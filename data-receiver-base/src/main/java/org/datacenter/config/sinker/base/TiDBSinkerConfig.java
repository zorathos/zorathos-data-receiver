package org.datacenter.config.sinker.base;

import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * @author : [wangminan]
 * @description : TiDB接收器配置
 */
@Data
@EqualsAndHashCode(callSuper = true)
public abstract class TiDBSinkerConfig extends BaseSinkerConfig {
    protected DataSinkType dataSinkType = DataSinkType.TIDB;
    protected String uri;
    protected String driverName;
    protected String username;
    protected String password;
}
