package org.datacenter.receiver.simulation;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.flink.util.function.SerializableFunction;
import org.datacenter.config.simulation.SimulationReceiverConfig;
import org.datacenter.model.base.TiDBTable;
import org.datacenter.model.simulation.ErrorData;
import org.datacenter.receiver.simulation.base.SimulationReceiver;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;


/**
 * @author : [宁]
 * @description : 仿真数据ErrorData的接收器
 */
@Slf4j
@Data
@EqualsAndHashCode(callSuper = true)
public class ErrorDataFileReceiver extends SimulationReceiver<ErrorData> {

    @Override
    public void prepare() {
        table = TiDBTable.ERROR_DATA;
        modelClass = ErrorData.class;
        super.prepare();
    }

    @Override
    protected SerializableFunction<CsvMapper, CsvSchema> getSchemaGenerator() {
        return mapper -> CsvSchema.builder()
                .addColumn("senderId")                // 发送方ID
                .addColumn("messageTime")             // 消息时间
                .addColumn("localTime")               // 本地时间
                .addColumn("messageSequenceNumber")   // 消息序列号
                .addColumn("messageId")               // 消息标识
                .addColumn("messageLength")           // 消息长度
                .addColumn("errorMessage")            // 错误信息
                .setUseHeader(true)
                .setColumnSeparator(',')
                .setLineSeparator("\n")
                .build();
    }

    @Override
    protected String getInsertQuery() {
        return """
                INSERT INTO `error_data` (
                    sortie_number, sender_id, message_time, local_time, message_sequence_number, message_id, message_length, error_message
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?
                );
                """;
    }

    @Override
    protected void bindPreparedStatement(PreparedStatement preparedStatement, ErrorData data, String sortieNumber) throws SQLException {
        // 注意 sortieNumber 是从配置里面来的 csv里面没有
        preparedStatement.setString(1, sortieNumber);
        preparedStatement.setString(2, data.getSenderId());
        // LocalTime -> java.sql.Time
        preparedStatement.setTime(3, data.getMessageTime() != null ? Time.valueOf(data.getMessageTime()) : null);
        // satelliteGuidanceTime is not present in ErrorData class
        preparedStatement.setTime(4, data.getLocalTime() != null ? Time.valueOf(data.getLocalTime()) : null);
        // Handle potential null for Long
        if (data.getMessageSequenceNumber() != null) {
            preparedStatement.setLong(5, data.getMessageSequenceNumber());
        } else {
            preparedStatement.setNull(5, java.sql.Types.BIGINT);
        }
        preparedStatement.setString(6, data.getMessageId());
        preparedStatement.setString(7, data.getMessageLength());
        preparedStatement.setString(8, data.getErrorMessage());
    }

    @Override
    public void start() {
        super.start();
    }

    // 参数输入形式为 --url s3://human-machine/simulation/simulated_data_large.csv --sortie_number 20250303_五_01_ACT-3_邱陈_J16_07#02
    public static void main(String[] args) {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        SimulationReceiverConfig config = new SimulationReceiverConfig(
                parameterTool.getRequired("url"),
                parameterTool.getRequired("sortie_number"));
        ErrorDataFileReceiver receiver = new ErrorDataFileReceiver();
        receiver.setConfig(config);
        receiver.run();
    }
}
