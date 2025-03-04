package org.datacenter.config.receiver.base.receiver;

import lombok.Getter;

/**
 * @author wangminan
 * @description 数据源类型 这玩意应该是一个用于校正的 不可配置的变量 所有数据源均应当在Flink能够接入的范围内
 */
@Getter
public enum DataSourceType {
    DATABASE_JDBC, // JDBC数据接收器
    DATABASE_CDC,  // 数据库CDC接收器
    KAFKA,         // Kafka数据接收器
    FILE;          // 离线文件数据接收器

    public static DataSourceType getDataSourceType(String dataSourceType) {
        for (DataSourceType type : DataSourceType.values()) {
            if (type.name().equalsIgnoreCase(dataSourceType)) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unsupported data source type: " + dataSourceType);
    }
}
