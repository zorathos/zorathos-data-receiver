package org.datacenter.receiver.physiological.input;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.flink.util.function.SerializableFunction;
import org.datacenter.config.receiver.physiological.PhysiologicalFileReceiverConfig;
import org.datacenter.model.base.TiDBTable;
import org.datacenter.model.physiological.input.TShirtHrv;
import org.datacenter.receiver.physiological.base.PhysiologicalFileReceiver;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import static org.datacenter.config.keys.HumanMachineReceiverConfigKey.IMPORT_ID;
import static org.datacenter.config.keys.HumanMachineReceiverConfigKey.PHYSIOLOGY_FILE_URL;

/**
 * @author : [wangminan]
 * @description : 心率变异性数据文件接收器
 */
public class TShirtHrvFileReceiver extends PhysiologicalFileReceiver<TShirtHrv> {

    @Override
    public void prepare() {
        table = TiDBTable.T_SHIRT_HRV;
        modelClass = TShirtHrv.class;
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
                .addColumn("rrMean")
                .addColumn("averageHeartRate")
                .addColumn("sdnn")
                .addColumn("rmssd")
                .addColumn("pnn50")
                .addColumn("sd1")
                .addColumn("sd2")
                .addColumn("sd1rsd2")
                .addColumn("apen")
                .addColumn("lfNorm")
                .addColumn("hfNorm")
                .addColumn("lfrhf")
                .addColumn("hf")
                .addColumn("lf")
                .addColumn("vlf")
                .addColumn("ulf")
                .addColumn("ttlpwr")
                .addColumn("rsa")
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
                    rr_mean, average_heart_rate, sdnn, rmssd, pnn50,
                    sd1, sd2, sd1rsd2, apen, lf_norm,
                    hf_norm, lfrhf, hf, lf, vlf,
                    ulf, ttlpwr, rsa, import_id
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?
                )
                """.formatted(table.getName());
    }

    @Override
    public void bindPreparedStatement(PreparedStatement preparedStatement, TShirtHrv data, Long importId) throws SQLException {
        preparedStatement.setLong(1, data.getRecordId());
        preparedStatement.setLong(2, data.getTaskId());
        preparedStatement.setLong(3, data.getDeviceId());
        preparedStatement.setTimestamp(4, data.getTimestamp() == null ?
                null : java.sql.Timestamp.valueOf(data.getTimestamp()));
        preparedStatement.setDouble(5, data.getSamplingRate());
        preparedStatement.setObject(6, data.getRrMean());
        preparedStatement.setObject(7, data.getAverageHeartRate());
        preparedStatement.setObject(8, data.getSdnn());
        preparedStatement.setObject(9, data.getRmssd());
        preparedStatement.setObject(10, data.getPnn50());
        preparedStatement.setObject(11, data.getSd1());
        preparedStatement.setObject(12, data.getSd2());
        preparedStatement.setObject(13, data.getSd1rsd2());
        preparedStatement.setObject(14, data.getApen());
        preparedStatement.setObject(15, data.getLfNorm());
        preparedStatement.setObject(16, data.getHfNorm());
        preparedStatement.setObject(17, data.getLfrhf());
        preparedStatement.setObject(18, data.getHf());
        preparedStatement.setObject(19, data.getLf());
        preparedStatement.setObject(20, data.getVlf());
        preparedStatement.setObject(21, data.getUlf());
        preparedStatement.setObject(22, data.getTtlpwr());
        preparedStatement.setObject(23, data.getRsa());
        preparedStatement.setLong(24, importId);
    }

    // 参数输入形式为 --url s3://human-machine/physiological/physiological_data_large.csv --importId 1
    public static void main(String[] args) {
        PhysiologicalFileReceiverConfig config = parseArgs(args);
        TShirtHrvFileReceiver receiver = new TShirtHrvFileReceiver();
        receiver.setConfig(config);
        receiver.run();
    }
}
