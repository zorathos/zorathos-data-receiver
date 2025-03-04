package org.datacenter.config.receiver.base.sinker;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

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
