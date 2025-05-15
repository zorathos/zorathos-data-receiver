package org.datacenter.receiver.physiological.input;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.flink.util.function.SerializableFunction;
import org.datacenter.config.receiver.physiological.PhysiologicalFileReceiverConfig;
import org.datacenter.model.base.TiDBTable;
import org.datacenter.model.physiological.input.TShirtEcgAccelGyro;
import org.datacenter.receiver.physiological.base.PhysiologicalFileReceiver;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author : [wangminan]
 * @description : TShirtEcgAccelGyro离线数据接收器 {@link TShirtEcgAccelGyro}
 */
public class TShirtEcgAccelGyroFileReceiver extends PhysiologicalFileReceiver<TShirtEcgAccelGyro> {

    @Override
    public void prepare() {
        table = TiDBTable.T_SHIRT_ECG_ACCEL_GYRO;
        modelClass = TShirtEcgAccelGyro.class;
        super.prepare();
    }

    @Override
    public SerializableFunction<CsvMapper, CsvSchema> getSchemaGenerator() {
        return mapper -> CsvSchema.builder()
                .addColumn("recordId")
                .addColumn("taskId")
                .addColumn("deviceId")
                .addColumn("timestamp")
                .addColumn("ecg1")
                .addColumn("ecg2")
                .addColumn("ecg3")
                .addColumn("accelX")
                .addColumn("accelY")
                .addColumn("accelZ")
                .addColumn("gyroX")
                .addColumn("gyroY")
                .addColumn("gyroZ")
                .setUseHeader(true)
                .setColumnSeparator(',')
                .setLineSeparator("\n")
                .build();
    }

    @Override
    public String getInsertQuery() {
        return """
                INSERT INTO %s (
                    record_id, task_id, device_id, timestamp,
                    ecg1, ecg2, ecg3,
                    accel_x, accel_y, accel_z,
                    gyro_x, gyro_y, gyro_z,
                    import_id
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                );
                """.formatted(table.getName());
    }

    @Override
    public void bindPreparedStatement(PreparedStatement preparedStatement, TShirtEcgAccelGyro data, Long importId) throws SQLException {
        preparedStatement.setLong(1, data.getRecordId());
        preparedStatement.setLong(2, data.getTaskId());
        preparedStatement.setLong(3, data.getDeviceId());
        preparedStatement.setTimestamp(4, data.getTimestamp() == null ?
                null : java.sql.Timestamp.valueOf(data.getTimestamp()));
        preparedStatement.setObject(5, data.getEcg1());
        preparedStatement.setObject(6, data.getEcg2());
        preparedStatement.setObject(7, data.getEcg3());
        preparedStatement.setObject(8, data.getAccelX());
        preparedStatement.setObject(9, data.getAccelY());
        preparedStatement.setObject(10, data.getAccelZ());
        preparedStatement.setObject(11, data.getGyroX());
        preparedStatement.setObject(12, data.getGyroY());
        preparedStatement.setObject(13, data.getGyroZ());
        preparedStatement.setLong(14, importId);
    }

    // 参数输入形式为 --url s3://human-machine/physiological/physiological_data_large.csv --importId 1
    public static void main(String[] args) {
        PhysiologicalFileReceiverConfig config = parseArgs(args);
        TShirtEcgAccelGyroFileReceiver receiver = new TShirtEcgAccelGyroFileReceiver();
        receiver.setConfig(config);
        receiver.run();
    }
}
