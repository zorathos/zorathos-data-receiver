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
import org.datacenter.model.simulation.Command;
import org.datacenter.receiver.simulation.base.SimulationReceiver;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;

import static org.datacenter.config.keys.HumanMachineReceiverConfigKey.SIMULATION_URL;
import static org.datacenter.config.keys.HumanMachineReceiverConfigKey.IMPORT_ID;
import static org.datacenter.config.keys.HumanMachineReceiverConfigKey.SIMULATION_BATCH_NUMBER;


/**
 * @author : [宁]
 * @description : 仿真数据Command的接收器
 */
@Slf4j
@Data
@EqualsAndHashCode(callSuper = true)
public class CommandFileReceiver extends SimulationReceiver<Command> {

    @Override
    public void prepare() {
        table = TiDBTable.COMMAND;
        modelClass = Command.class;
        super.prepare();
    }

    @Override
    protected SerializableFunction<CsvMapper, CsvSchema> getSchemaGenerator() {
        return mapper -> CsvSchema.builder()
                .addColumn("aircraftId")              // 飞机ID
                .addColumn("messageTime")             // 消息时间
                .addColumn("satelliteGuidanceTime")   // 卫导时间
                .addColumn("localTime")               // 本地时间
                .addColumn("messageSequenceNumber")   // 消息序列号
                .addColumn("commandType")             // 导调/导调回复
                .addColumn("commandId")               // 命令ID
                .addColumn("commandContent")          // 命令内容
                .addColumn("responseSequenceNumber")  // 回复序列号
                .setUseHeader(true)
                .setColumnSeparator(',')
                .setLineSeparator("\n")
                .build();
    }

    @Override
    protected String getInsertQuery() {
        return """
                INSERT INTO `command` (
                    import_id,batch_number, aircraft_id, message_time, satellite_guidance_time, local_time, message_sequence_number, command_type, command_id, command_content, response_sequence_number
                ) VALUES (
                    ?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                );
                """;
    }

    @Override
    protected void bindPreparedStatement(PreparedStatement preparedStatement, Command data, String batchNumber, long importId) throws SQLException {
        preparedStatement.setLong(1, importId);        preparedStatement.setString(2, batchNumber);
        preparedStatement.setString(3, data.getAircraftId());
        // LocalTime -> java.sql.Time
        preparedStatement.setTime(4, data.getMessageTime() != null ? Time.valueOf(data.getMessageTime()) : null);
        preparedStatement.setTime(5, data.getSatelliteGuidanceTime() != null ? Time.valueOf(data.getSatelliteGuidanceTime()) : null);
        preparedStatement.setTime(6, data.getLocalTime() != null ? Time.valueOf(data.getLocalTime()) : null);
        if (data.getMessageSequenceNumber() != null) {
            preparedStatement.setLong(7, data.getMessageSequenceNumber());
        } else {
            preparedStatement.setNull(7, java.sql.Types.BIGINT);
        }
        preparedStatement.setString(8, data.getCommandType());
        preparedStatement.setString(9, data.getCommandId());
        preparedStatement.setString(10, data.getCommandContent());
        if (data.getResponseSequenceNumber() != null) {
            preparedStatement.setLong(11, data.getResponseSequenceNumber());
        } else {
            preparedStatement.setNull(11, java.sql.Types.BIGINT);
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
                parameterTool.getRequired(SIMULATION_URL.getKeyForParamsMap()),
                parameterTool.getRequired(IMPORT_ID.getKeyForParamsMap()),
                parameterTool.getRequired(SIMULATION_BATCH_NUMBER.getKeyForParamsMap()));
        CommandFileReceiver receiver = new CommandFileReceiver();
        receiver.setConfig(config);
        receiver.run();
    }
}
