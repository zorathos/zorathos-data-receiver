package org.datacenter.receiver.util;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.datacenter.config.HumanMachineConfig;
import org.datacenter.model.base.TiDBDatabase;

import static org.datacenter.config.keys.HumanMachineSysConfigKey.FLINK_JDBC_SINKER_BATCH_INTERVAL;
import static org.datacenter.config.keys.HumanMachineSysConfigKey.FLINK_JDBC_SINKER_BATCH_SIZE;
import static org.datacenter.config.keys.HumanMachineSysConfigKey.FLINK_JDBC_SINKER_MAX_RETRIES;
import static org.datacenter.config.keys.HumanMachineSysConfigKey.TIDB_CONNECTION_CHECK_TIMEOUT_SECONDS;
import static org.datacenter.config.keys.HumanMachineSysConfigKey.TIDB_MYSQL_DRIVER_NAME;
import static org.datacenter.config.keys.HumanMachineSysConfigKey.TIDB_PASSWORD;
import static org.datacenter.config.keys.HumanMachineSysConfigKey.TIDB_URL_PREFIX;
import static org.datacenter.config.keys.HumanMachineSysConfigKey.TIDB_URL_SUFFIX;
import static org.datacenter.config.keys.HumanMachineSysConfigKey.TIDB_USERNAME;

/**
 * @author : [wangminan]
 * @description : TiDB持久化Util
 */
public class JdbcSinkUtil {
    public static final String TIDB_URL_HUMAN_MACHINE =
            HumanMachineConfig.getProperty(TIDB_URL_PREFIX) +
                    TiDBDatabase.HUMAN_MACHINE.getName() + HumanMachineConfig.getProperty(TIDB_URL_SUFFIX);

    public static final String TIDB_URL_FLIGHT_PLAN =
            HumanMachineConfig.getProperty(TIDB_URL_PREFIX) +
                    TiDBDatabase.FLIGHT_PLAN.getName() + HumanMachineConfig.getProperty(TIDB_URL_SUFFIX);

    public static final String TIDB_URL_SIMULATION =
            HumanMachineConfig.getProperty(TIDB_URL_PREFIX) +
                    TiDBDatabase.SIMULATION.getName() + HumanMachineConfig.getProperty(TIDB_URL_SUFFIX);

    public static final String TIDB_URL_SORTIES =
            HumanMachineConfig.getProperty(TIDB_URL_PREFIX) +
                    TiDBDatabase.SORTIES.getName() + HumanMachineConfig.getProperty(TIDB_URL_SUFFIX);

    public static final String TIDB_REAL_WORLD_FLIGHT =
            HumanMachineConfig.getProperty(TIDB_URL_PREFIX) +
                    TiDBDatabase.REAL_WORLD_FLIGHT.getName() + HumanMachineConfig.getProperty(TIDB_URL_SUFFIX);

    public static JdbcExecutionOptions getTiDBJdbcExecutionOptions() {
        return JdbcExecutionOptions.builder()
                .withBatchSize(Integer.parseInt(HumanMachineConfig.getProperty(FLINK_JDBC_SINKER_BATCH_SIZE)))
                .withBatchIntervalMs(Integer.parseInt(HumanMachineConfig.getProperty(FLINK_JDBC_SINKER_BATCH_INTERVAL)))
                .withMaxRetries(Integer.parseInt(HumanMachineConfig.getProperty(FLINK_JDBC_SINKER_MAX_RETRIES)))
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
                .withUrl(HumanMachineConfig.getProperty(TIDB_URL_PREFIX) + database.getName() + HumanMachineConfig.getProperty(TIDB_URL_SUFFIX))
                .withDriverName(HumanMachineConfig.getProperty(TIDB_MYSQL_DRIVER_NAME))
                .withUsername(HumanMachineConfig.getProperty(TIDB_USERNAME))
                .withPassword(HumanMachineConfig.getProperty(TIDB_PASSWORD))
                .withConnectionCheckTimeoutSeconds(Integer.parseInt(HumanMachineConfig.getProperty(TIDB_CONNECTION_CHECK_TIMEOUT_SECONDS)))
                .build();
    }

    public static JdbcConnectionOptions getTiDBJdbcConnectionOptions(String url) {
        return new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(url)
                .withDriverName(HumanMachineConfig.getProperty(TIDB_MYSQL_DRIVER_NAME))
                .withUsername(HumanMachineConfig.getProperty(TIDB_USERNAME))
                .withPassword(HumanMachineConfig.getProperty(TIDB_PASSWORD))
                .withConnectionCheckTimeoutSeconds(Integer.parseInt(HumanMachineConfig.getProperty(TIDB_CONNECTION_CHECK_TIMEOUT_SECONDS)))
                .build();
    }
}
