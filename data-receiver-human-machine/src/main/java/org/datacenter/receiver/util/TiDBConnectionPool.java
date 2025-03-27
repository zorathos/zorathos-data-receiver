package org.datacenter.receiver.util;

import com.alibaba.druid.pool.DruidDataSource;
import lombok.Getter;
import org.datacenter.model.base.TiDBDatabase;

import java.sql.Connection;
import java.sql.SQLException;

import static org.datacenter.config.system.BaseSysConfig.humanMachineProperties;

/**
 * @author : [wangminan]
 * @description : 连接到TiDB数据库连接池 (使用DruidDataSource实现)
 */
public class TiDBConnectionPool {
    private final DruidDataSource dataSource;

    @Getter
    private static TiDBConnectionPool instance;

    public TiDBConnectionPool(TiDBDatabase database) {
        // 创建Druid数据源
        dataSource = new DruidDataSource();

        // 设置基本连接属性
        dataSource.setDriverClassName(humanMachineProperties.getProperty("tidb.driverName"));
        dataSource.setUrl(
                humanMachineProperties.getProperty("tidb.url.prefix") +
                database.getName() +
                humanMachineProperties.getProperty("tidb.url.suffix"));
        dataSource.setUsername(humanMachineProperties.getProperty("tidb.username"));
        dataSource.setPassword(humanMachineProperties.getProperty("tidb.password"));

        // 配置连接池参数
        dataSource.setMaxActive(Integer.parseInt(humanMachineProperties.getProperty("tidb.pool.maxTotal")));
        dataSource.setMinIdle(Integer.parseInt(humanMachineProperties.getProperty("tidb.pool.minIdle")));
        dataSource.setMaxActive(Integer.parseInt(humanMachineProperties.getProperty("tidb.pool.maxIdle")));

        // 配置连接检测
        dataSource.setTestOnBorrow(true);
        dataSource.setTestOnReturn(true);
        dataSource.setValidationQuery("SELECT 1");

        // 其他常用配置
        dataSource.setInitialSize(5);
        dataSource.setMaxWait(60000);
        dataSource.setTimeBetweenEvictionRunsMillis(60000);
        dataSource.setMinEvictableIdleTimeMillis(300000);

        // 设置实例
        instance = this;
    }

    /**
     * 获取数据库连接
     * 保持方法签名不变，但内部实现改为使用DruidDataSource
     */
    public Connection getConnection() throws Exception {
        try {
            return dataSource.getConnection();
        } catch (SQLException e) {
            throw new Exception("获取数据库连接失败", e);
        }
    }

    /**
     * 归还连接
     * 在Druid中不需要手动归还，只需关闭连接即可
     */
    public void returnConnection(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                // 记录日志或者处理异常
            }
        }
    }

    /**
     * 关闭连接池
     */
    public void closePool() {
        if (dataSource != null) {
            dataSource.close();
        }
    }
}
