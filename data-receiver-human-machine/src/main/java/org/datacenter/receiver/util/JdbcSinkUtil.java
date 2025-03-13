package org.datacenter.receiver.util;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;

import static org.datacenter.config.system.BaseSysConfig.humanMachineProperties;

/**
 * @author : [wangminan]
 * @description : TiDB持久化Util
 */
public class JdbcSinkUtil {
    public static JdbcExecutionOptions getTiDBJdbcExecutionOptions() {
        return JdbcExecutionOptions.builder()
                .withBatchSize(Integer.parseInt(humanMachineProperties.getProperty("flink.jdbc.sinker.batchSize")))
                .withBatchIntervalMs(Integer.parseInt(humanMachineProperties.getProperty("flink.jdbc.sinker.batchInterval")))
                .withMaxRetries(Integer.parseInt(humanMachineProperties.getProperty("flink.jdbc.sinker.maxRetries")))
                .build();
    }

    public static JdbcConnectionOptions getTiDBJdbcConnectionOptions() {
        return new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(humanMachineProperties.getProperty("tidb.url.humanMachine"))
                .withDriverName(humanMachineProperties.getProperty("tidb.driverName"))
                .withUsername(humanMachineProperties.getProperty("tidb.username"))
                .withPassword(humanMachineProperties.getProperty("tidb.password"))
                .withConnectionCheckTimeoutSeconds(Integer.parseInt(humanMachineProperties.getProperty("tidb.connectionCheckTimeoutSeconds")))
                .build();
    }
}
