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
import org.datacenter.model.simulation.PL17Rtkn;
import org.datacenter.receiver.simulation.base.SimulationReceiver;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;

import static org.datacenter.config.keys.HumanMachineReceiverConfigKey.IMPORT_ID;
import static org.datacenter.config.keys.HumanMachineReceiverConfigKey.SIMULATION_BATCH_NUMBER;
import static org.datacenter.config.keys.HumanMachineReceiverConfigKey.SIMULATION_URL;


/**
 * @author : [宁]
 * @description : 仿真数据PL17Rtkn的接收器
 */
@Slf4j
@Data
@EqualsAndHashCode(callSuper = true)
public class PL17RtknFileReceiver extends SimulationReceiver<PL17Rtkn> {

    @Override
    public void prepare() {
        table = TiDBTable.PL17_RTKN;
        modelClass = PL17Rtkn.class;
        super.prepare();
    }

    @Override
    protected SerializableFunction<CsvMapper, CsvSchema> getSchemaGenerator() {
        return mapper -> CsvSchema.builder()
                .addColumn("aircraftId")                  // 飞机ID
                .addColumn("messageTime")                 // 消息时间
                .addColumn("satelliteGuidanceTime")       // 卫导时间
                .addColumn("localTime")                   // 本地时间
                .addColumn("messageSequenceNumber")       // 消息序列号
                .addColumn("weaponId")                    // 武器ID
                .addColumn("weaponType")                  // 武器类型
                .addColumn("targetId")                    // 目标ID
                .addColumn("targetRealOrVirtual")         // 目标机实虚属性
                .addColumn("hitResult")                   // 命中结果
                .addColumn("missReason")                  // 未命中原因
                .addColumn("missDistance")                // 脱靶量
                .addColumn("matchingFailureReason")       // 匹配失败原因
                .addColumn("jammingEffective")            // 干扰是否有效
                .addColumn("jamming")                     // 干扰
                .addColumn("afterburner")                 // 加力
                .addColumn("headOn")                      // 迎头
                .addColumn("heading")                     // 航向
                .addColumn("pitch")                       // 俯仰
                .setUseHeader(true)
                .setColumnSeparator(',')
                .setLineSeparator("\n")
                .build();
    }

    @Override
    protected String getInsertQuery() {
        return """
                INSERT INTO `pl17_rtkn` (
                    import_id,batch_number, aircraft_id, message_time, satellite_guidance_time, local_time, message_sequence_number, weapon_id, weapon_type, target_id, target_real_or_virtual, 
                    hit_result, miss_reason, miss_distance, matching_failure_reason, jamming_effective, jamming, afterburner, head_on, heading, pitch
                ) VALUES (
                    ?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                );
                """;
    }

    @Override
    protected void bindPreparedStatement(PreparedStatement preparedStatement, PL17Rtkn data, String batchNumber, long importId) throws SQLException {
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
        preparedStatement.setString(8, data.getWeaponId());
        preparedStatement.setString(9, data.getWeaponType());
        preparedStatement.setString(10, data.getTargetId());
        preparedStatement.setString(11, data.getTargetRealOrVirtual());
        preparedStatement.setString(12, data.getHitResult());
        preparedStatement.setString(13, data.getMissReason());
        preparedStatement.setString(14, data.getMissDistance());
        preparedStatement.setString(15, data.getMatchingFailureReason());
        preparedStatement.setString(16, data.getJammingEffective());
        preparedStatement.setString(17, data.getJamming());
        preparedStatement.setString(18, data.getAfterburner());
        preparedStatement.setString(19, data.getHeadOn());
        preparedStatement.setString(20, data.getHeading());
        preparedStatement.setString(21, data.getPitch());
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
                parameterTool.getRequired(IMPORT_ID.getKeyForParamsMap()),
                parameterTool.getRequired(SIMULATION_BATCH_NUMBER.getKeyForParamsMap()));
        PL17RtknFileReceiver receiver = new PL17RtknFileReceiver();
        receiver.setConfig(config);
        receiver.run();
    }
}
