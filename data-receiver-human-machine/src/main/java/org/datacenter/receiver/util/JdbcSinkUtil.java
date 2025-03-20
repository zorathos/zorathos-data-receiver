package org.datacenter.receiver.util;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.datacenter.model.base.TiDBDatabase;

import static org.datacenter.config.system.BaseSysConfig.humanMachineProperties;

/**
 * @author : [wangminan]
 * @description : TiDB持久化Util
 */
public class JdbcSinkUtil {
    public static final String TIDB_URL_HUMAN_MACHINE =
            humanMachineProperties.getProperty("tidb.url.base") +
                    TiDBDatabase.HUMAN_MACHINE.getName() +
                    humanMachineProperties.getProperty("tidb.url.suffix");

    public static final String TIDB_URL_FLIGHT_PLAN =
            humanMachineProperties.getProperty("tidb.url.base") +
                    TiDBDatabase.FLIGHT_PLAN.getName() +
                    humanMachineProperties.getProperty("tidb.url.suffix");

    public static JdbcExecutionOptions getTiDBJdbcExecutionOptions() {
        return JdbcExecutionOptions.builder()
                .withBatchSize(Integer.parseInt(humanMachineProperties.getProperty("flink.jdbc.sinker.batchSize")))
                .withBatchIntervalMs(Integer.parseInt(humanMachineProperties.getProperty("flink.jdbc.sinker.batchInterval")))
                .withMaxRetries(Integer.parseInt(humanMachineProperties.getProperty("flink.jdbc.sinker.maxRetries")))
                .build();
    }

    public static JdbcConnectionOptions getTiDBJdbcConnectionOptions(TiDBDatabase database) {
        return new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(humanMachineProperties.get("tidb.url.base") + database.getName() + humanMachineProperties.get("tidb.url.suffix"))
                .withDriverName(humanMachineProperties.getProperty("tidb.driverName"))
                .withUsername(humanMachineProperties.getProperty("tidb.username"))
                .withPassword(humanMachineProperties.getProperty("tidb.password"))
                .withConnectionCheckTimeoutSeconds(Integer.parseInt(humanMachineProperties.getProperty("tidb.connectionCheckTimeoutSeconds")))
                .build();
    }

    public static JdbcConnectionOptions getTiDBJdbcConnectionOptions(String url) {
        return new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(url)
                .withDriverName(humanMachineProperties.getProperty("tidb.driverName"))
                .withUsername(humanMachineProperties.getProperty("tidb.username"))
                .withPassword(humanMachineProperties.getProperty("tidb.password"))
                .withConnectionCheckTimeoutSeconds(Integer.parseInt(humanMachineProperties.getProperty("tidb.connectionCheckTimeoutSeconds")))
                .build();
    }
}
