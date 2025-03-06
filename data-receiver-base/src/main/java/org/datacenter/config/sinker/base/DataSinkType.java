package org.datacenter.config.sinker.base;

import lombok.Getter;

@Getter
public enum DataSinkType {
    TIDB,
    MINIO;

    public static DataSinkType fromString(String type) {
        for (DataSinkType sinkType : DataSinkType.values()) {
            if (sinkType.name().equalsIgnoreCase(type)) {
                return sinkType;
            }
        }
        return null;
    }
}
