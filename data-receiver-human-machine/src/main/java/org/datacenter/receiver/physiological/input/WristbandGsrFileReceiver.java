package org.datacenter.receiver.physiological.input;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.flink.util.function.SerializableFunction;
import org.datacenter.config.receiver.physiological.PhysiologicalFileReceiverConfig;
import org.datacenter.model.base.TiDBTable;
import org.datacenter.model.physiological.input.WristBandSpo2;
import org.datacenter.model.physiological.input.WristbandGsr;
import org.datacenter.receiver.physiological.base.PhysiologicalFileReceiver;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import static org.datacenter.config.keys.HumanMachineReceiverConfigKey.IMPORT_ID;
import static org.datacenter.config.keys.HumanMachineReceiverConfigKey.PHYSIOLOGY_FILE_URL;

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
                .addColumn("samplingRate")
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
                    record_id, task_id, device_id, timestamp, sampling_rate,
                    gsr_data, import_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """.formatted(table.getName());
    }

    @Override
    public void bindPreparedStatement(PreparedStatement preparedStatement, WristbandGsr data, Long importId) throws SQLException {
        preparedStatement.setLong(1, data.getRecordId());
        preparedStatement.setLong(2, data.getTaskId());
        preparedStatement.setLong(3, data.getDeviceId());
        preparedStatement.setTimestamp(4, data.getTimestamp() == null ?
                null : java.sql.Timestamp.valueOf(data.getTimestamp()));
        preparedStatement.setDouble(5, data.getSamplingRate());
        preparedStatement.setObject(6, data.getGsrData());
        preparedStatement.setLong(7, importId);
    }

    // 参数输入形式为 --url s3://human-machine/physiological/physiological_data_large.csv --importId 1
    public static void main(String[] args) {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        PhysiologicalFileReceiverConfig config = PhysiologicalFileReceiverConfig.builder()
                .importId(Long.valueOf(parameterTool.getRequired(IMPORT_ID.getKeyForParamsMap())))
                .url(parameterTool.getRequired(PHYSIOLOGY_FILE_URL.getKeyForParamsMap()))
                .build();
        WristbandGsrFileReceiver receiver = new WristbandGsrFileReceiver();
        receiver.setConfig(config);
        receiver.run();
    }
}
