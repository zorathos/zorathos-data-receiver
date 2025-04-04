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
            humanMachineProperties.getProperty("tidb.url.prefix") +
                    TiDBDatabase.HUMAN_MACHINE.getName() +
                    humanMachineProperties.getProperty("tidb.url.suffix");

    public static final String TIDB_URL_FLIGHT_PLAN =
            humanMachineProperties.getProperty("tidb.url.prefix") +
                    TiDBDatabase.FLIGHT_PLAN.getName() +
                    humanMachineProperties.getProperty("tidb.url.suffix");

    public static final String TIDB_URL_SIMULATION =
            humanMachineProperties.getProperty("tidb.url.prefix") +
                    TiDBDatabase.SIMULATION.getName() +
                    humanMachineProperties.getProperty("tidb.url.suffix");

    public static final String TIDB_URL_SORTIES =
            humanMachineProperties.getProperty("tidb.url.prefix") +
                    TiDBDatabase.SORTIES.getName() +
                    humanMachineProperties.getProperty("tidb.url.suffix");

    public static final String TIDB_REAL_WORLD_FLIGHT =
            humanMachineProperties.getProperty("tidb.url.prefix") +
                    TiDBDatabase.REAL_WORLD_FLIGHT.getName() +
                    humanMachineProperties.getProperty("tidb.url.suffix");

    public static JdbcExecutionOptions getTiDBJdbcExecutionOptions() {
        return JdbcExecutionOptions.builder()
                .withBatchSize(Integer.parseInt(humanMachineProperties.getProperty("flink.jdbc.sinker.batchSize")))
                .withBatchIntervalMs(Integer.parseInt(humanMachineProperties.getProperty("flink.jdbc.sinker.batchInterval")))
                .withMaxRetries(Integer.parseInt(humanMachineProperties.getProperty("flink.jdbc.sinker.maxRetries")))
                .build();
    }

    /**
     * AtLeastOnceSink
     * TiDB不支持XA语法 仅在内部使用XA事务
     *
     * @param database 数据库
     * @return JdbcConnectionOptions
     */
    public static JdbcConnectionOptions getTiDBJdbcConnectionOptions(TiDBDatabase database) {
        return new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(humanMachineProperties.get("tidb.url.prefix") + database.getName() + humanMachineProperties.get("tidb.url.suffix"))
                .withDriverName(humanMachineProperties.getProperty("tidb.mysql.driverName"))
                .withUsername(humanMachineProperties.getProperty("tidb.username"))
                .withPassword(humanMachineProperties.getProperty("tidb.password"))
                .withConnectionCheckTimeoutSeconds(Integer.parseInt(humanMachineProperties.getProperty("tidb.connectionCheckTimeoutSeconds")))
                .build();
    }

    public static JdbcConnectionOptions getTiDBJdbcConnectionOptions(String url) {
        return new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(url)
                .withDriverName(humanMachineProperties.getProperty("tidb.mysql.driverName"))
                .withUsername(humanMachineProperties.getProperty("tidb.username"))
                .withPassword(humanMachineProperties.getProperty("tidb.password"))
                .withConnectionCheckTimeoutSeconds(Integer.parseInt(humanMachineProperties.getProperty("tidb.connectionCheckTimeoutSeconds")))
                .build();
    }
}
