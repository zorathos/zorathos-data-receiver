package org.datacenter.receiver.physiological.input;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.flink.util.function.SerializableFunction;
import org.datacenter.config.receiver.physiological.PhysiologicalFileReceiverConfig;
import org.datacenter.model.base.TiDBTable;
import org.datacenter.model.physiological.input.TShirtHeartRate;
import org.datacenter.receiver.physiological.base.PhysiologicalFileReceiver;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author : [wangminan]
 * @description : [一句话描述该类的功能]
 */
public class TShirtHeartRateFileReceiver extends PhysiologicalFileReceiver<TShirtHeartRate> {
    @Override
    public void prepare() {
        table = TiDBTable.T_SHIRT_HEART_RATE;
        modelClass = TShirtHeartRate.class;
        super.prepare();
    }

    @Override
    public SerializableFunction<CsvMapper, CsvSchema> getSchemaGenerator() {
        return mapper -> CsvSchema.builder()
                .addColumn("taskId")
                .addColumn("deviceId")
                .addColumn("pilotId")
                .addColumn("timestamp")
                .addColumn("heartRate")
                .setUseHeader(true)
                .setColumnSeparator(',')
                .setLineSeparator("\n")
                .build();
    }

    @Override
    public String getInsertQuery() {
        return """
                INSERT INTO %s (
                    pilot_id, task_id, device_id, timestamp,
                    heart_rate, import_id
                ) VALUES (?, ?, ?, ?, ?, ?)
                """.formatted(table.getName());
    }

    @Override
    public void bindPreparedStatement(PreparedStatement preparedStatement, TShirtHeartRate data, Long importId) throws SQLException {
        preparedStatement.setLong(1, data.getPilotId());
        preparedStatement.setLong(2, data.getTaskId());
        preparedStatement.setLong(3, data.getDeviceId());
        preparedStatement.setTimestamp(4, data.getTimestamp() == null ?
                null : java.sql.Timestamp.valueOf(data.getTimestamp()));
        preparedStatement.setObject(5, data.getHeartRate());
        preparedStatement.setLong(6, importId);
    }

    // 参数输入形式为 --url s3://human-machine/physiological/physiological_data_large.csv --importId 1
    public static void main(String[] args) {
        PhysiologicalFileReceiverConfig config = parseArgs(args);
        TShirtHeartRateFileReceiver receiver = new TShirtHeartRateFileReceiver();
        receiver.setConfig(config);
        receiver.run();
    }
}
