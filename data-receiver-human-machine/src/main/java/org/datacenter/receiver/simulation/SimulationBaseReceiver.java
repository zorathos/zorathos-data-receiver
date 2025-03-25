package org.datacenter.receiver.simulation;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.jdbc.sink.JdbcSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.csv.CsvReaderFormat;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.function.SerializableFunction;
import org.apache.flink.util.function.SerializableSupplier;
import org.datacenter.config.simulation.SimulationReceiverConfig;
import org.datacenter.exception.ZorathosException;
import org.datacenter.model.base.TiDBDatabase;
import org.datacenter.model.base.TiDBTable;
import org.datacenter.receiver.BaseReceiver;
import org.datacenter.receiver.util.DataReceiverUtil;
import org.datacenter.receiver.util.JdbcSinkUtil;

import java.io.Serial;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static org.datacenter.config.system.BaseSysConfig.humanMachineProperties;

/**
 * @author : [ning]
 * @description : 仿真数据接收器基类
 */
@Slf4j
@Data
@EqualsAndHashCode(callSuper = true)
public abstract class SimulationBaseReceiver<T> extends BaseReceiver implements Serializable {
    @Serial
    private static final long serialVersionUID = 1231445L;

    protected SimulationReceiverConfig config;
    protected TiDBTable table;
    protected transient Class<T> modelClass;


    @Override
    public void prepare() {
        super.prepare();

        // 通过JDBC连接到对应j表 如果有和config.getSortieNumber()相同的记录就删除
        try {
            log.info("Linking to table: {}.{} for preparation.", TiDBDatabase.SIMULATION.getName(), table.getName());
            Class.forName(humanMachineProperties.getProperty("tidb.driverName"));
            Connection connection = DriverManager.getConnection(
                    JdbcSinkUtil.TIDB_URL_SIMULATION,
                    humanMachineProperties.getProperty("tidb.username"),
                    humanMachineProperties.getProperty("tidb.password"));
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
                    log.info("Delete {} records from table: {}.{}.", count, TiDBDatabase.SIMULATION.getName(), table.getName());
                } else {
                    // 如果没有对应记录 新建分区
                    String createPartitionSql = """
                            ALTER TABLE `%s` ADD PARTITION PARTITIONS 1;
                            """.formatted(table.getName());
                    var createPreparedStatement = connection.prepareStatement(createPartitionSql);
                    createPreparedStatement.executeUpdate();
                    log.info("Create partition for table: {}.{}.", TiDBDatabase.SIMULATION.getName(), table.getName());
                }
            }
            connection.close();
            log.info("Preparation finished.");
        } catch (SQLException | ClassNotFoundException e) {
            throw new ZorathosException(e, "Error occurs while preparing the" + table.getName() + "table.");
        }
    }

    // 定义抽象方法：获取CSV列定义
    protected abstract SerializableFunction<CsvMapper, CsvSchema> getSchemaGenerator();

    // 定义抽象方法：获取插入SQL语句
    protected abstract String getInsertQuery();

    // 定义抽象方法：绑定PreparedStatement参数
    protected abstract void bindPreparedStatement(PreparedStatement preparedStatement, T data, String sortieNumber) throws SQLException;

    // 简化版的JdbcStatementBuilder获取方法
    protected JdbcStatementBuilder<T> getJdbcStatementBuilder() {
        // 保存为final变量以避免捕获外部实例
        final String sortieNumber = config.getSortieNumber();

        return (statement, data) -> bindPreparedStatement(statement, data, sortieNumber);
    }

    // 构建CsvReaderFormat
    protected CsvReaderFormat<T> buildCsvReaderFormat() {
        return CsvReaderFormat.forSchema(
                (SerializableSupplier<CsvMapper>) () -> {
                    CsvMapper csvMapper = new CsvMapper();
                    // 注册JavaTimeModule以支持日期时间类型
                    csvMapper.registerModule(new JavaTimeModule());
                    return csvMapper;
                },
                getSchemaGenerator(),
                TypeInformation.of(modelClass)
        );
    }

    // 构建FileSource
    protected FileSource<T> buildFileSource(CsvReaderFormat<T> csvReaderFormat) {
        return FileSource.forRecordStreamFormat(csvReaderFormat, new Path(config.getUrl())).build();
    }

    // 构建JdbcSink
    protected JdbcSink<T> buildJdbcSink() {
        return JdbcSink.<T>builder()
                .withQueryStatement(getInsertQuery(), getJdbcStatementBuilder())
                .withExecutionOptions(JdbcSinkUtil.getTiDBJdbcExecutionOptions())
                .buildAtLeastOnce(JdbcSinkUtil.getTiDBJdbcConnectionOptions(TiDBDatabase.SIMULATION));
    }

    @Override
    public void start() {
        StreamExecutionEnvironment env = DataReceiverUtil.prepareStreamEnv();
        CsvReaderFormat<T> csvReaderFormat = buildCsvReaderFormat();

        FileSource<T> fileSource = buildFileSource(csvReaderFormat);
        JdbcSink<T> sink = buildJdbcSink();

        env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), modelClass.getName() + " File Source")
                .returns(modelClass)
                .sinkTo(sink)
                .name(modelClass.getName() + " File Sink");
        try {
            env.execute(modelClass.getName() + " File Receiver");

        } catch (Exception e) {
            throw new ZorathosException(e, "Error occurs while executing the Flink job.");
        }
    }
}
