package org.datacenter.receiver.physiological.base;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.datacenter.config.physiological.PhysiologicalFileReceiverConfig;
import org.datacenter.exception.ZorathosException;
import org.datacenter.model.base.TiDBDatabase;
import org.datacenter.receiver.CsvFileReceiver;
import org.datacenter.receiver.util.MySQLDriverConnectionPool;

import java.io.Serial;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author : [wangminan]
 * @description : 抽象的生理数据离线接入器
 */
@Slf4j
@Data
@EqualsAndHashCode(callSuper = true)
public abstract class PhysiologicalFileReceiver<T> extends CsvFileReceiver<T, PhysiologicalFileReceiverConfig> implements Serializable {

    @Serial
    private static final long serialVersionUID = 8345278L;
    private MySQLDriverConnectionPool physiologicalConnectionPool = new MySQLDriverConnectionPool(TiDBDatabase.PHYSIOLOGICAL);

    @Override
    public void prepare() {
        super.prepare();
        database = TiDBDatabase.PHYSIOLOGICAL;
        url = config.getUrl();
        // 通过JDBC连接到对应j表 如果有和config.getSortieNumber()相同的记录就删除
        try {
            log.info("Linking to table: {}.{} for preparation.", database.getName(), table.getName());
            Connection connection = physiologicalConnectionPool.getConnection();
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
            physiologicalConnectionPool.returnConnection(connection);
            log.info("Preparation finished.");
        } catch (SQLException e) {
            throw new ZorathosException(e, "Error occurs while preparing the" + table.getName() + "table.");
        }
    }

    @Override
    protected JdbcStatementBuilder<T> getJdbcStatementBuilder() {
        final  String sortieNumber = config.getSortieNumber();
        final  String sensorId =
        return null;
    }
}
