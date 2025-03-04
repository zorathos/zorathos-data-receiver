package org.datacenter.config.receiver.base.sinker;

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
