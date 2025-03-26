package org.datacenter.receiver.util;

import lombok.AllArgsConstructor;
import lombok.Builder;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * @author : [wangminan]
 * @description : TiDB连接池工厂
 */
@Builder
@AllArgsConstructor
public class TiDBConnectionFactory implements PooledObjectFactory<Connection> {

    private String driverClassName;
    private String url;
    private String username;
    private String password;

    @Override
    public PooledObject<Connection> makeObject() throws Exception {
        // 显式加载驱动
        Class.forName(driverClassName);
        Connection connection = DriverManager.getConnection(url, username, password);
        return new DefaultPooledObject<>(connection);
    }

    @Override
    public void destroyObject(PooledObject<Connection> p) throws Exception {
        Connection connection = p.getObject();
        if (!connection.isClosed()) {
            connection.close();
        }
    }

    @Override
    public boolean validateObject(PooledObject<Connection> p) {
        try {
            return !p.getObject().isClosed() && p.getObject().isValid(1);
        } catch (SQLException e) {
            return false;
        }
    }

    @Override
    public void activateObject(PooledObject<Connection> p) throws Exception {
        // No-op
    }

    @Override
    public void passivateObject(PooledObject<Connection> p) throws Exception {
        // No-op
    }
}
