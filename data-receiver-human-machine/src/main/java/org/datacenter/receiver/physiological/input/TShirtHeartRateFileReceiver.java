package org.datacenter.receiver.physiological.input;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.flink.util.function.SerializableFunction;
import org.datacenter.config.receiver.physiological.PhysiologicalFileReceiverConfig;
import org.datacenter.model.base.TiDBTable;
import org.datacenter.model.physiological.input.TShirtHeartRate;
import org.datacenter.model.physiological.input.TShirtResp;
import org.datacenter.receiver.physiological.base.PhysiologicalFileReceiver;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import static org.datacenter.config.keys.HumanMachineReceiverConfigKey.IMPORT_ID;
import static org.datacenter.config.keys.HumanMachineReceiverConfigKey.PHYSIOLOGY_FILE_URL;

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
                .addColumn("recordId")
                .addColumn("taskId")
                .addColumn("deviceId")
                .addColumn("timestamp")
                .addColumn("samplingRate")
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
                    record_id, task_id, device_id, timestamp, sampling_rate,
                    heart_rate, import_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """.formatted(table.getName());
    }

    @Override
    public void bindPreparedStatement(PreparedStatement preparedStatement, TShirtHeartRate data, Long importId) throws SQLException {
        preparedStatement.setLong(1, data.getRecordId());
        preparedStatement.setLong(2, data.getTaskId());
        preparedStatement.setLong(3, data.getDeviceId());
        preparedStatement.setTimestamp(4, data.getTimestamp() == null ?
                null : java.sql.Timestamp.valueOf(data.getTimestamp()));
        preparedStatement.setDouble(5, data.getSamplingRate());
        preparedStatement.setObject(6, data.getHeartRate());
        preparedStatement.setLong(7, importId);
    }

    // 参数输入形式为 --url s3://human-machine/physiological/physiological_data_large.csv --importId 1
    public static void main(String[] args) {
        PhysiologicalFileReceiverConfig config = parseArgs(args);
        TShirtHeartRateFileReceiver receiver = new TShirtHeartRateFileReceiver();
        receiver.setConfig(config);
        receiver.run();
    }
}
