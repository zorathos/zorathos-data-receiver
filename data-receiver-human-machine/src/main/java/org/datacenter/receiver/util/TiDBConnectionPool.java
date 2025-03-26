package org.datacenter.receiver.util;

import lombok.Getter;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.datacenter.model.base.TiDBDatabase;

import java.sql.Connection;

import static org.datacenter.config.system.BaseSysConfig.humanMachineProperties;

/**
 * @author : [wangminan]
 * @description : 连接到TiDB数据库连接池
 */
public class TiDBConnectionPool {
    private final GenericObjectPool<Connection> pool;

    @Getter
    private static TiDBConnectionPool instance;

    public TiDBConnectionPool(TiDBDatabase database) {
        TiDBConnectionFactory factory = TiDBConnectionFactory.builder()
                .driverClassName(humanMachineProperties.getProperty("tidb.driverName"))
                .url(humanMachineProperties.getProperty("tidb.url.prefix") +
                        database.getName() +
                        humanMachineProperties.getProperty("tidb.url.suffix"))
                .username(humanMachineProperties.getProperty("tidb.username"))
                .password(humanMachineProperties.getProperty("tidb.password"))
                .build();
        GenericObjectPoolConfig<Connection> config = new GenericObjectPoolConfig<>();
        config.setMaxTotal(Integer.parseInt(humanMachineProperties.getProperty("tidb.pool.maxTotal")));
        config.setMinIdle(Integer.parseInt(humanMachineProperties.getProperty("tidb.pool.minIdle")));
        config.setMaxIdle(Integer.parseInt(humanMachineProperties.getProperty("tidb.pool.maxIdle")));
        config.setTestOnBorrow(true);
        config.setTestOnReturn(true);
        this.pool = new GenericObjectPool<>(factory, config);
        instance = this;
    }

    public Connection getConnection() throws Exception {
        return pool.borrowObject();
    }

    public void returnConnection(Connection connection) {
        if (connection != null) {
            pool.returnObject(connection);
        }
    }

    public void closePool() {
        pool.close();
    }
}
