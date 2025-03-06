package org.datacenter.config.sinker.base;

import lombok.Data;

/**
 * @author : [wangminan]
 * @description : 基础的持久化配置
 */
@Data
public abstract class BaseSinkerConfig {
    protected DataSinkType dataSinkType;
}
