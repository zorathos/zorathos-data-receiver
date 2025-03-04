package org.datacenter.config.receiver.base.sinker;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

/**
 * @author : [wangminan]
 * /@description : Minio接收器配置
 */
@Data
@EqualsAndHashCode(callSuper = true)
public abstract class MinioSinkerConfig extends BaseSinkerConfig {
    protected DataSinkType dataSinkType = DataSinkType.MINIO;
}
