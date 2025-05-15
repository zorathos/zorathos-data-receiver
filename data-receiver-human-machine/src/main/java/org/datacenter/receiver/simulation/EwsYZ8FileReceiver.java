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
import org.datacenter.model.simulation.EwsYZ8;
import org.datacenter.receiver.simulation.base.SimulationReceiver;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;

import static org.datacenter.config.keys.HumanMachineReceiverConfigKey.IMPORT_ID;
import static org.datacenter.config.keys.HumanMachineReceiverConfigKey.SIMULATION_BATCH_NUMBER;
import static org.datacenter.config.keys.HumanMachineReceiverConfigKey.SIMULATION_URL;


/**
 * @author : [宁]
 * @description : 仿真数据EwsYZ8的接收器
 */
@Slf4j
@Data
@EqualsAndHashCode(callSuper = true)
public class EwsYZ8FileReceiver extends SimulationReceiver<EwsYZ8> {

    @Override
    public void prepare() {
        table = TiDBTable.EWS_YZ8;
        modelClass = EwsYZ8.class;
        super.prepare();
    }

    @Override
    protected SerializableFunction<CsvMapper, CsvSchema> getSchemaGenerator() {
        return mapper -> CsvSchema.builder()
                .addColumn("aircraftId")                   // 飞机ID
                .addColumn("messageTime")                  // 消息时间
                .addColumn("satelliteGuidanceTime")        // 卫导时间
                .addColumn("localTime")                    // 本地时间
                .addColumn("messageSequenceNumber")        // 消息序列号
                .addColumn("workingStatus")                // 工作状态
                .addColumn("omnidirectionalDetectionQ1")   // 全向探测Q1频段
                .addColumn("omnidirectionalDetectionQ2")   // 全向探测Q2频段
                .addColumn("preciseDirectionFindingJ1")    // 精测向J1频段
                .addColumn("preciseDirectionFindingJ2")    // 精测向J2频段
                .addColumn("preciseDirectionFindingJ3")    // 精测向J3频段
                .addColumn("highGainG1")                   // 高增益G1频段
                .addColumn("highGainG2")                   // 高增益G2频段
                .setUseHeader(true)
                .setColumnSeparator(',')
                .setLineSeparator("\n")
                .build();
    }

    @Override
    protected String getInsertQuery() {
        return """
                INSERT INTO `ews_yz8` (
                    import_id,batch_number, aircraft_id, message_time, satellite_guidance_time, local_time, message_sequence_number, working_status, omnidirectional_detection_q1, omnidirectional_detection_q2, precise_direction_finding_j1, 
                    precise_direction_finding_j2, precise_direction_finding_j3, high_gain_g1, high_gain_g2
                ) VALUES (
                    ?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                    ?, ?, ?, ?
                );
                """;
    }

    @Override
    protected void bindPreparedStatement(PreparedStatement preparedStatement, EwsYZ8 data, String batchNumber, long importId) throws SQLException {
        preparedStatement.setLong(1, importId);        preparedStatement.setString(2, batchNumber);
        preparedStatement.setString(3, data.getAircraftId());
        preparedStatement.setTime(4, data.getMessageTime() != null ? Time.valueOf(data.getMessageTime()) : null);
        preparedStatement.setTime(5, data.getSatelliteGuidanceTime() != null ? Time.valueOf(data.getSatelliteGuidanceTime()) : null);
        preparedStatement.setTime(6, data.getLocalTime() != null ? Time.valueOf(data.getLocalTime()) : null);
        if (data.getMessageSequenceNumber() != null) {
            preparedStatement.setLong(7, data.getMessageSequenceNumber());
        } else {
            preparedStatement.setNull(7, java.sql.Types.BIGINT);
        }
        preparedStatement.setString(8, data.getWorkingStatus());
        preparedStatement.setString(9, data.getOmnidirectionalDetectionQ1());
        preparedStatement.setString(10, data.getOmnidirectionalDetectionQ2());
        preparedStatement.setString(11, data.getPreciseDirectionFindingJ1());
        preparedStatement.setString(12, data.getPreciseDirectionFindingJ2());
        preparedStatement.setString(13, data.getPreciseDirectionFindingJ3());
        preparedStatement.setString(14, data.getHighGainG1());
        preparedStatement.setString(15, data.getHighGainG2());
    }

    @Override
    public void start() {
        super.start();
    }

    // 参数输入形式为 --url s3://human-machine/simulation/simulated_data_large.csv --import_id 12345 --batch_number 20250303_五_01_ACT-3_邱陈_J16_07#02
    public static void main(String[] args) {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        SimulationReceiverConfig config = new SimulationReceiverConfig(
                parameterTool.getRequired(SIMULATION_URL.getKeyForParamsMap()),
                Long.parseLong(parameterTool.getRequired(IMPORT_ID.getKeyForParamsMap())),
                parameterTool.getRequired(SIMULATION_BATCH_NUMBER.getKeyForParamsMap()));
        EwsYZ8FileReceiver receiver = new EwsYZ8FileReceiver();
        receiver.setConfig(config);
        receiver.run();
    }
}
