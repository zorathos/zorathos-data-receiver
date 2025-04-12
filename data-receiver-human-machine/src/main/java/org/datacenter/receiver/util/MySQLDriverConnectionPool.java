package org.datacenter.receiver.util;

import com.alibaba.druid.pool.DruidDataSource;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.datacenter.config.HumanMachineSysConfig;
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
                HumanMachineSysConfig.getHumanMachineProperties().getProperty("tidb.url.prefix") + database.getName() + HumanMachineSysConfig.getHumanMachineProperties().getProperty("tidb.url.suffix"),
                HumanMachineSysConfig.getHumanMachineProperties().getProperty("tidb.username"),
                HumanMachineSysConfig.getHumanMachineProperties().getProperty("tidb.password")
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
        dataSource.setDriverClassName(HumanMachineSysConfig.getHumanMachineProperties().getProperty("tidb.mysql.driverName"));
        dataSource.setUrl(url);
        dataSource.setUsername(username);
        dataSource.setPassword(password);
        configureDataSource();
        log.info("TiDB connection pool initialized");
    }

    private void configureDataSource() {
        dataSource.setMaxActive(Integer.parseInt(HumanMachineSysConfig.getHumanMachineProperties().getProperty("tidb.pool.maxTotal")));
        dataSource.setMinIdle(Integer.parseInt(HumanMachineSysConfig.getHumanMachineProperties().getProperty("tidb.pool.minIdle")));
        dataSource.setMaxActive(Integer.parseInt(HumanMachineSysConfig.getHumanMachineProperties().getProperty("tidb.pool.maxIdle")));
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
