package org.datacenter.receiver.util;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.datacenter.config.system.HumanMachineSysConfig;
import org.datacenter.model.base.TiDBDatabase;

/**
 * @author : [wangminan]
 * @description : TiDB持久化Util
 */
public class JdbcSinkUtil {
    public static final String TIDB_URL_HUMAN_MACHINE =
            HumanMachineSysConfig.getHumanMachineProperties().getProperty("tidb.url.prefix") +
                    TiDBDatabase.HUMAN_MACHINE.getName() +
                    HumanMachineSysConfig.getHumanMachineProperties().getProperty("tidb.url.suffix");

    public static final String TIDB_URL_FLIGHT_PLAN =
            HumanMachineSysConfig.getHumanMachineProperties().getProperty("tidb.url.prefix") +
                    TiDBDatabase.FLIGHT_PLAN.getName() +
                    HumanMachineSysConfig.getHumanMachineProperties().getProperty("tidb.url.suffix");

    public static final String TIDB_URL_SIMULATION =
            HumanMachineSysConfig.getHumanMachineProperties().getProperty("tidb.url.prefix") +
                    TiDBDatabase.SIMULATION.getName() +
                    HumanMachineSysConfig.getHumanMachineProperties().getProperty("tidb.url.suffix");

    public static final String TIDB_URL_SORTIES =
            HumanMachineSysConfig.getHumanMachineProperties().getProperty("tidb.url.prefix") +
                    TiDBDatabase.SORTIES.getName() +
                    HumanMachineSysConfig.getHumanMachineProperties().getProperty("tidb.url.suffix");

    public static final String TIDB_REAL_WORLD_FLIGHT =
            HumanMachineSysConfig.getHumanMachineProperties().getProperty("tidb.url.prefix") +
                    TiDBDatabase.REAL_WORLD_FLIGHT.getName() +
                    HumanMachineSysConfig.getHumanMachineProperties().getProperty("tidb.url.suffix");

    public static JdbcExecutionOptions getTiDBJdbcExecutionOptions() {
        return JdbcExecutionOptions.builder()
                .withBatchSize(Integer.parseInt(HumanMachineSysConfig.getHumanMachineProperties().getProperty("flink.jdbc.sinker.batchSize")))
                .withBatchIntervalMs(Integer.parseInt(HumanMachineSysConfig.getHumanMachineProperties().getProperty("flink.jdbc.sinker.batchInterval")))
                .withMaxRetries(Integer.parseInt(HumanMachineSysConfig.getHumanMachineProperties().getProperty("flink.jdbc.sinker.maxRetries")))
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
                .withUrl(HumanMachineSysConfig.getHumanMachineProperties().get("tidb.url.prefix") + database.getName() + HumanMachineSysConfig.getHumanMachineProperties().get("tidb.url.suffix"))
                .withDriverName(HumanMachineSysConfig.getHumanMachineProperties().getProperty("tidb.mysql.driverName"))
                .withUsername(HumanMachineSysConfig.getHumanMachineProperties().getProperty("tidb.username"))
                .withPassword(HumanMachineSysConfig.getHumanMachineProperties().getProperty("tidb.password"))
                .withConnectionCheckTimeoutSeconds(Integer.parseInt(HumanMachineSysConfig.getHumanMachineProperties().getProperty("tidb.connectionCheckTimeoutSeconds")))
                .build();
    }

    public static JdbcConnectionOptions getTiDBJdbcConnectionOptions(String url) {
        return new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(url)
                .withDriverName(HumanMachineSysConfig.getHumanMachineProperties().getProperty("tidb.mysql.driverName"))
                .withUsername(HumanMachineSysConfig.getHumanMachineProperties().getProperty("tidb.username"))
                .withPassword(HumanMachineSysConfig.getHumanMachineProperties().getProperty("tidb.password"))
                .withConnectionCheckTimeoutSeconds(Integer.parseInt(HumanMachineSysConfig.getHumanMachineProperties().getProperty("tidb.connectionCheckTimeoutSeconds")))
                .build();
    }
}
