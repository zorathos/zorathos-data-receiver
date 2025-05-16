package org.datacenter.receiver.environment.input;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.flink.util.function.SerializableFunction;
import org.datacenter.config.receiver.environment.EnvironmentFileReceiverConfig;
import org.datacenter.model.base.TiDBTable;
import org.datacenter.model.environment.input.TemperatureHumidity;
import org.datacenter.receiver.environment.base.EnvironmentFileReceiver;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author : [wangminan]
 * @description : 温度与湿度传感器数据接收器
 */
public class TemperatureHumidityFileReceiver extends EnvironmentFileReceiver<TemperatureHumidity> {
    @Override
    public void prepare() {
        table = TiDBTable.TEMPERATURE_HUMIDITY;
        modelClass = TemperatureHumidity.class;
        super.prepare();
    }

    @Override
    protected SerializableFunction<CsvMapper, CsvSchema> getSchemaGenerator() {
        return mapper -> CsvSchema.builder()
                .addColumn("taskId")
                .addColumn("deviceId")
                .addColumn("timestamp")
                .addColumn("temperature")
                .addColumn("humidity")
                .setUseHeader(true)
                .setColumnSeparator(',')
                .setLineSeparator("\n")
                .build();
    }

    @Override
    protected String getInsertQuery() {
        return """
                INSERT INTO temperature_humidity (
                    task_id, device_id, timestamp, import_id,
                    temperature, humidity
                ) VALUES (
                    ?, ?, ?, ?,
                    ?, ?
                );
                """;
    }

    @Override
    public void bindPreparedStatement(PreparedStatement preparedStatement, TemperatureHumidity data, Long importId) throws SQLException {
        preparedStatement.setLong(1, data.getTaskId());
        preparedStatement.setLong(2, data.getDeviceId());
        preparedStatement.setTimestamp(3, data.getTimestamp() == null ?
                null : java.sql.Timestamp.valueOf(data.getTimestamp()));
        preparedStatement.setLong(4, importId);
        preparedStatement.setObject(5, data.getTemperature());
        preparedStatement.setObject(6, data.getHumidity());
    }

    public static void main(String[] args) {
        EnvironmentFileReceiverConfig config = parseArgs(args);
        TemperatureHumidityFileReceiver receiver = new TemperatureHumidityFileReceiver();
        receiver.setConfig(config);
        receiver.run();
    }
}
