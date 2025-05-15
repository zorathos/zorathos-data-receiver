package org.datacenter.receiver.physiological.input;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.flink.util.function.SerializableFunction;
import org.datacenter.config.receiver.physiological.PhysiologicalFileReceiverConfig;
import org.datacenter.model.base.TiDBTable;
import org.datacenter.model.physiological.input.WristbandPpgAccel;
import org.datacenter.receiver.physiological.base.PhysiologicalFileReceiver;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author : [wangminan]
 * @description : 腕带红光、红外光和加速度数据文件接收器
 */
public class WristbandPpgAccelFileReceiver extends PhysiologicalFileReceiver<WristbandPpgAccel> {

    @Override
    public void prepare() {
        table = TiDBTable.WRISTBAND_PPG_ACCEL;
        modelClass = WristbandPpgAccel.class;
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
                .addColumn("ppgRed1")
                .addColumn("ppgRed2")
                .addColumn("ppgRed3")
                .addColumn("ppgRed4")
                .addColumn("ppgInfrared1")
                .addColumn("ppgInfrared2")
                .addColumn("ppgInfrared3")
                .addColumn("ppgInfrared4")
                .addColumn("accelX")
                .addColumn("accelY")
                .addColumn("accelZ")
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
                    ppg_red1, ppg_red2, ppg_red3, ppg_red4,
                    ppg_infrared1, ppg_infrared2, ppg_infrared3, ppg_infrared4,
                    accel_x, accel_y, accel_z,
                    import_id
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
                """.formatted(table.getName());
    }

    @Override
    public void bindPreparedStatement(PreparedStatement preparedStatement, WristbandPpgAccel data, Long importId) throws SQLException {
        preparedStatement.setLong(1, data.getRecordId());
        preparedStatement.setLong(2, data.getTaskId());
        preparedStatement.setLong(3, data.getDeviceId());
        preparedStatement.setTimestamp(4, data.getTimestamp() == null ?
                null : java.sql.Timestamp.valueOf(data.getTimestamp()));
        preparedStatement.setDouble(5, data.getSamplingRate());
        preparedStatement.setString(6, data.getPpgRed1());
        preparedStatement.setString(7, data.getPpgRed2());
        preparedStatement.setString(8, data.getPpgRed3());
        preparedStatement.setString(9, data.getPpgRed4());
        preparedStatement.setString(10, data.getPpgInfrared1());
        preparedStatement.setString(11, data.getPpgInfrared2());
        preparedStatement.setString(12, data.getPpgInfrared3());
        preparedStatement.setString(13, data.getPpgInfrared4());
        preparedStatement.setObject(14, data.getAccelX());
        preparedStatement.setObject(15, data.getAccelY());
        preparedStatement.setObject(16, data.getAccelZ());
        preparedStatement.setLong(17, importId);
    }

    // 参数输入形式为 --url s3://human-machine/physiological/physiological_data_large.csv --importId 1
    public static void main(String[] args) {
        PhysiologicalFileReceiverConfig config = parseArgs(args);
        WristbandPpgAccelFileReceiver receiver = new WristbandPpgAccelFileReceiver();
        receiver.setConfig(config);
        receiver.run();
    }
}
