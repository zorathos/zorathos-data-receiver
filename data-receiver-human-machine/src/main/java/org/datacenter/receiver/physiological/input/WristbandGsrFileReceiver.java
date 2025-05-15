package org.datacenter.receiver.physiological.input;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.flink.util.function.SerializableFunction;
import org.datacenter.config.receiver.physiological.PhysiologicalFileReceiverConfig;
import org.datacenter.model.base.TiDBTable;
import org.datacenter.model.physiological.input.WristbandGsr;
import org.datacenter.receiver.physiological.base.PhysiologicalFileReceiver;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author : [wangminan]
 * @description : [一句话描述该类的功能]
 */
public class WristbandGsrFileReceiver extends PhysiologicalFileReceiver<WristbandGsr> {
    @Override
    public void prepare() {
        table = TiDBTable.WRISTBAND_GSR;
        modelClass = WristbandGsr.class;
        super.prepare();
    }

    @Override
    public SerializableFunction<CsvMapper, CsvSchema> getSchemaGenerator() {
        return mapper -> CsvSchema.builder()
                .addColumn("recordId")
                .addColumn("taskId")
                .addColumn("deviceId")
                .addColumn("timestamp")
                .addColumn("gsrData")
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
                    gsr_data, import_id
                ) VALUES (?, ?, ?, ?, ?, ?)
                """.formatted(table.getName());
    }

    @Override
    public void bindPreparedStatement(PreparedStatement preparedStatement, WristbandGsr data, Long importId) throws SQLException {
        preparedStatement.setLong(1, data.getRecordId());
        preparedStatement.setLong(2, data.getTaskId());
        preparedStatement.setLong(3, data.getDeviceId());
        preparedStatement.setTimestamp(4, data.getTimestamp() == null ?
                null : java.sql.Timestamp.valueOf(data.getTimestamp()));
        preparedStatement.setObject(5, data.getGsrData());
        preparedStatement.setLong(6, importId);
    }

    // 参数输入形式为 --url s3://human-machine/physiological/physiological_data_large.csv --importId 1
    public static void main(String[] args) {
        PhysiologicalFileReceiverConfig config = parseArgs(args);
        WristbandGsrFileReceiver receiver = new WristbandGsrFileReceiver();
        receiver.setConfig(config);
        receiver.run();
    }
}
