package org.datacenter.receiver;

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
import org.datacenter.config.receiver.BaseReceiverConfig;
import org.datacenter.exception.ZorathosException;
import org.datacenter.model.base.TiDBDatabase;
import org.datacenter.model.base.TiDBTable;
import org.datacenter.receiver.util.DataReceiverUtil;
import org.datacenter.receiver.util.JdbcSinkUtil;

import java.io.Serial;
import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.SQLException;


/**
 * @author : [ning]
 * @description : CSV数据接收器基类
 */
@Slf4j
@Data
@EqualsAndHashCode(callSuper = true)
public abstract class CsvFileReceiver<T, C extends BaseReceiverConfig> extends BaseReceiver implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;
    protected TiDBDatabase database;
    protected C config;
    protected TiDBTable table;
    protected transient Class<T> modelClass;
    protected String url;


    @Override
    public void prepare() {
        super.prepare();
    }

    // 定义抽象方法：获取CSV列定义
    protected abstract SerializableFunction<CsvMapper, CsvSchema> getSchemaGenerator();

    // 定义抽象方法：获取插入SQL语句
    protected abstract String getInsertQuery();

    // 定义抽象方法：绑定PreparedStatement参数
    protected abstract void bindPreparedStatement(PreparedStatement preparedStatement, T data, String batchNumber, long importId) throws SQLException;

    // 简化版的JdbcStatementBuilder获取方法
    protected abstract JdbcStatementBuilder<T> getJdbcStatementBuilder();

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
        return FileSource.forRecordStreamFormat(csvReaderFormat, new Path(url)).build();
    }

    // 构建JdbcSink
    protected JdbcSink<T> buildJdbcSink() {
        return JdbcSink.<T>builder()
                .withQueryStatement(getInsertQuery(), getJdbcStatementBuilder())
                .withExecutionOptions(JdbcSinkUtil.getTiDBJdbcExecutionOptions())
                .buildAtLeastOnce(JdbcSinkUtil.getTiDBJdbcConnectionOptions(database));
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
