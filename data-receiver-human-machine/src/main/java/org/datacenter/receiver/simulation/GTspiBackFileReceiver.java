package org.datacenter.receiver.simulation;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.flink.util.function.SerializableFunction;
import org.datacenter.config.receiver.simulation.SimulationReceiverConfig;
import org.datacenter.model.base.TiDBTable;
import org.datacenter.model.simulation.GTspiBack;
import org.datacenter.receiver.simulation.base.SimulationReceiver;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;

import static org.datacenter.config.keys.HumanMachineReceiverConfigKey.IMPORT_ID;
import static org.datacenter.config.keys.HumanMachineReceiverConfigKey.SIMULATION_BATCH_NUMBER;
import static org.datacenter.config.keys.HumanMachineReceiverConfigKey.SIMULATION_URL;


/**
 * @author : [宁]
 * @description : 仿真数据GTspiBack的接收器
 */
@Slf4j
@Data
@EqualsAndHashCode(callSuper = true)
public class GTspiBackFileReceiver extends SimulationReceiver<GTspiBack> {

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
        return """
                INSERT INTO `g_tspi_back` (
                    import_id,batch_number, ground_defense_id, message_time, satellite_guidance_time, local_time, message_sequence_number, response_message_sequence_number
                ) VALUES (
                    ?,?, ?, ?, ?, ?, ?, ?
                );
                """;
    }

    @Override
    protected void bindPreparedStatement(PreparedStatement preparedStatement, GTspiBack data, String batchNumber, long importId) throws SQLException {
        preparedStatement.setLong(1, importId);        preparedStatement.setString(2, batchNumber);
        preparedStatement.setString(3, data.getGroundDefenseId());
        preparedStatement.setTime(4, data.getMessageTime() != null ? Time.valueOf(data.getMessageTime()) : null);
        preparedStatement.setTime(5, data.getSatelliteGuidanceTime() != null ? Time.valueOf(data.getSatelliteGuidanceTime()) : null);
        preparedStatement.setTime(6, data.getLocalTime() != null ? Time.valueOf(data.getLocalTime()) : null);
        if (data.getMessageSequenceNumber() != null) {
            preparedStatement.setLong(7, data.getMessageSequenceNumber());
        } else {
            preparedStatement.setNull(7, java.sql.Types.BIGINT);
        }
        if (data.getResponseMessageSequenceNumber() != null) {
            preparedStatement.setLong(8, data.getResponseMessageSequenceNumber());
        } else {
            preparedStatement.setNull(8, java.sql.Types.BIGINT);
        }
    }

    @Override
    public void start() {
        super.start();
    }

    // 参数输入形式为 --url s3://human-machine/simulation/simulated_data_large.csv --import_id 20250303 --batch_number 五_01_ACT-3_邱陈_J16_07#02
    public static void main(String[] args) {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        SimulationReceiverConfig config = new SimulationReceiverConfig(
                parameterTool.getRequired(SIMULATION_URL.getKeyForParamsMap()),
                parameterTool.getRequired(IMPORT_ID.getKeyForParamsMap()),
                parameterTool.getRequired(SIMULATION_BATCH_NUMBER.getKeyForParamsMap()));
        GTspiBackFileReceiver receiver = new GTspiBackFileReceiver();
        receiver.setConfig(config);
        receiver.run();
    }
}
