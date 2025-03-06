package org.datacenter.config.sinker.base;

import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * @author : [wangminan]
 * /@description : Minio接收器配置
 */
@Data
@EqualsAndHashCode(callSuper = true)
public abstract class MinioSinkerConfig extends BaseSinkerConfig {
    protected DataSinkType dataSinkType = DataSinkType.MINIO;
}
