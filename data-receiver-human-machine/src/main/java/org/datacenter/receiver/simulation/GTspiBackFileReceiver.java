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
import org.datacenter.model.simulation.GTspiBack;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;


/**
 * @author : [宁]
 * @description : 仿真数据GTspiBack的接收器
 */
@Slf4j
@Data
@EqualsAndHashCode(callSuper = true)
public class GTspiBackFileReceiver extends SimulationBaseReceiver<GTspiBack> {

    @Override
    public void prepare() {
        table = TiDBTable.GT_SPI_BACK;
        modelClass = GTspiBack.class;
        super.prepare();
    }

    @Override
    protected SerializableFunction<CsvMapper, CsvSchema> getSchemaGenerator() {
        return mapper -> CsvSchema.builder()
                .addColumn("groundDefenseId")             // 地防ID
                .addColumn("messageTime")                 // 消息时间
                .addColumn("satelliteGuidanceTime")       // 卫导时间
                .addColumn("localTime")                   // 本地时间
                .addColumn("messageSequenceNumber")       // 消息序列号
                .addColumn("responseMessageSequenceNumber") // 回复消息序列号
                .setUseHeader(true)
                .setColumnSeparator(',')
                .setLineSeparator("\n")
                .build();
    }

    @Override
    protected String getInsertQuery() {
        return"""
INSERT INTO `g_tspi_back` (
    sortie_number, ground_defense_id, message_time, satellite_guidance_time, local_time, message_sequence_number, response_message_sequence_number
) VALUES (
    ?, ?, ?, ?, ?, ?, ?
);
""" ;
    }

    @Override
    protected void bindPreparedStatement(PreparedStatement preparedStatement, GTspiBack data, String sortieNumber) throws SQLException {
        // 注意 sortieNumber 是从配置里面来的 csv里面没有
        preparedStatement.setString(1, sortieNumber);
        preparedStatement.setString(2, data.getGroundDefenseId());
        // LocalTime -> java.sql.Time
        preparedStatement.setTime(3, data.getMessageTime() != null ? Time.valueOf(data.getMessageTime()) : null);
        preparedStatement.setTime(4, data.getSatelliteGuidanceTime() != null ? Time.valueOf(data.getSatelliteGuidanceTime()) : null);
        preparedStatement.setTime(5, data.getLocalTime() != null ? Time.valueOf(data.getLocalTime()) : null);
        // Handle potential null for Long (messageSequenceNumber)
        if (data.getMessageSequenceNumber() != null) {
            preparedStatement.setLong(6, data.getMessageSequenceNumber());
        } else {
            preparedStatement.setNull(6, java.sql.Types.BIGINT);
        }
        // Handle potential null for Long (responseMessageSequenceNumber)
        if (data.getResponseMessageSequenceNumber() != null) {
            preparedStatement.setLong(7, data.getResponseMessageSequenceNumber());
        } else {
            preparedStatement.setNull(7, java.sql.Types.BIGINT);
        }
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
        GTspiBackFileReceiver receiver = new GTspiBackFileReceiver();
        receiver.setConfig(config);
        receiver.run();
    }
}