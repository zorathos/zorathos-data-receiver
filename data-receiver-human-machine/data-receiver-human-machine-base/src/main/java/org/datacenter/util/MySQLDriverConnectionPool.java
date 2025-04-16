package org.datacenter.util;

import com.alibaba.druid.pool.DruidDataSource;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.datacenter.config.HumanMachineConfig;
import org.datacenter.config.keys.HumanMachineSysConfigKey;
import org.datacenter.exception.ZorathosException;
import org.datacenter.model.base.TiDBDatabase;

import java.sql.Connection;
import java.sql.SQLException;


@Slf4j
public class MySQLDriverConnectionPool {
    private final DruidDataSource dataSource;

    @Getter
    private static MySQLDriverConnectionPool instance;

    public MySQLDriverConnectionPool(TiDBDatabase database) {
        this(
                HumanMachineConfig.getProperty(HumanMachineSysConfigKey.TIDB_URL_PREFIX) + database.getName() + HumanMachineConfig.getProperty(HumanMachineSysConfigKey.TIDB_URL_SUFFIX),
                HumanMachineConfig.getProperty(HumanMachineSysConfigKey.TIDB_USERNAME),
                HumanMachineConfig.getProperty(HumanMachineSysConfigKey.TIDB_PASSWORD)
        );
        log.info("TiDB connection pool for database: {} is initializing", database.getName());
        instance = this;
        try {
            dataSource.init();
        } catch (SQLException e) {
            throw new ZorathosException(e, "Failed to initialize connection pool");
        }
    }

    public MySQLDriverConnectionPool(String url, String username, String password) {
        dataSource = new DruidDataSource();
        dataSource.setDriverClassName(HumanMachineConfig.getProperty(HumanMachineSysConfigKey.TIDB_MYSQL_DRIVER_NAME));
        dataSource.setUrl(url);
        dataSource.setUsername(username);
        dataSource.setPassword(password);
        configureDataSource();
        log.info("TiDB connection pool initialized");
    }

    private void configureDataSource() {
        dataSource.setMaxActive(Integer.parseInt(HumanMachineConfig.getProperty(HumanMachineSysConfigKey.TIDB_POOL_MAX_TOTAL)));
        dataSource.setMinIdle(Integer.parseInt(HumanMachineConfig.getProperty(HumanMachineSysConfigKey.TIDB_POOL_MIN_IDLE)));
        dataSource.setMaxActive(Integer.parseInt(HumanMachineConfig.getProperty(HumanMachineSysConfigKey.TIDB_POOL_MAX_IDLE)));
        dataSource.setTestOnBorrow(true);
        dataSource.setTestOnReturn(true);
        dataSource.setValidationQuery("SELECT 1");
        dataSource.setInitialSize(5);
        dataSource.setMaxWait(60000);
        dataSource.setTimeBetweenEvictionRunsMillis(60000);
        dataSource.setMinEvictableIdleTimeMillis(300000);
    }

    public Connection getConnection() {
        try {
            return dataSource.getConnection();
        } catch (SQLException e) {
            throw new ZorathosException(e);
        }
    }

    public void returnConnection(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                log.error(e.getMessage());
            }
        }
    }

    public void closePool() {
        if (dataSource != null) {
            dataSource.close();
        }
    }
}
