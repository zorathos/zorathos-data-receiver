package org.datacenter.receiver.simulation.base;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.datacenter.config.receiver.simulation.SimulationReceiverConfig;
import org.datacenter.exception.ZorathosException;
import org.datacenter.model.base.TiDBDatabase;
import org.datacenter.receiver.CsvFileReceiver;
import org.datacenter.receiver.util.MySQLDriverConnectionPool;

import java.io.Serial;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author : [ning]
 * @description : 仿真数据接收器基类
 */
@Slf4j
@Data
@EqualsAndHashCode(callSuper = true)
public abstract class SimulationReceiver<T> extends CsvFileReceiver<T, SimulationReceiverConfig> implements Serializable {

    @Serial
    private static final long serialVersionUID = 2345678L;

    @Override
    public void prepare() {
        super.prepare();
        database = TiDBDatabase.SIMULATION;
        MySQLDriverConnectionPool simulationConnectionPool = new MySQLDriverConnectionPool(TiDBDatabase.SIMULATION);
        url = config.getUrl();
        // 通过JDBC连接到对应j表 如果有和config.getSortieNumber()相同的记录就删除
        try {
            log.info("Linking to table: {}.{} for preparation.", database.getName(), table.getName());
            Connection connection = simulationConnectionPool.getConnection();
            String selectSql = """
                    SELECT COUNT(*) FROM `%s` WHERE `sortie_number` = ?;
                    """.formatted(table.getName());
            var preparedStatement = connection.prepareStatement(selectSql);
            preparedStatement.setString(1, config.getSortieNumber());
            var resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                int count = resultSet.getInt(1);
                if (count > 0) {
                    // 如果有对应记录 就删除
                    String deleteSql = """
                            DELETE FROM `%s` WHERE `sortie_number` = ?;
                            """.formatted(table.getName());
                    var deletePreparedStatement = connection.prepareStatement(deleteSql);
                    deletePreparedStatement.setString(1, config.getSortieNumber());
                    deletePreparedStatement.executeUpdate();
                    log.info("Delete {} records from table: {}.{}.", count, database.getName(), table.getName());
                } else {
                    // 如果没有对应记录 新建分区
                    String createPartitionSql = """
                            ALTER TABLE `%s` ADD PARTITION PARTITIONS 1;
                            """.formatted(table.getName());
                    var createPreparedStatement = connection.prepareStatement(createPartitionSql);
                    createPreparedStatement.executeUpdate();
                    log.info("Create partition for table: {}.{}.", database.getName(), table.getName());
                }
            }
            simulationConnectionPool.returnConnection(connection);
            log.info("Preparation finished.");
        } catch (SQLException e) {
            throw new ZorathosException(e, "Error occurs while preparing the" + table.getName() + "table.");
        }
    }

    @Override
    protected JdbcStatementBuilder<T> getJdbcStatementBuilder() {
        final String sortieNumber = config.getSortieNumber();

        return (statement, data) -> bindPreparedStatement(statement, data, sortieNumber);
    }
}
