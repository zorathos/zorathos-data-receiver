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
import org.datacenter.model.simulation.Rtkn;
import org.datacenter.receiver.simulation.base.SimulationReceiver;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;

import static org.datacenter.config.keys.HumanMachineReceiverConfigKey.SIMULATION_SORTIE_NUMBER;
import static org.datacenter.config.keys.HumanMachineReceiverConfigKey.SIMULATION_URL;


/**
 * @author : [宁]
 * @description : 仿真数据Rtkn的接收器
 */
@Slf4j
@Data
@EqualsAndHashCode(callSuper = true)
public class RtknFileReceiver extends SimulationReceiver<Rtkn> {

    @Override
    public void prepare() {
        table = TiDBTable.RTKN;
        modelClass = Rtkn.class;
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
                .addColumn("interceptedWeaponId")         // 被拦截武器ID
                .addColumn("hitResult")                   // 命中结果
                .addColumn("missReason")                  // 未命中原因
                .addColumn("missDistance")                // 脱靶量
                .addColumn("matchingFailureReason")       // 匹配失败原因
                .addColumn("groundDefenseEquipmentType")  // 地导装备类型
                .addColumn("groundDefenseEquipmentId")    // 地导装备ID
                .addColumn("groundDefenseEquipmentType1") // 地导装备类型.1
                .addColumn("groundDefenseEquipmentId1")   // 地导装备ID.1
                .addColumn("groundDefenseEquipmentType2") // 地导装备类型.2
                .addColumn("groundDefenseEquipmentId2")   // 地导装备ID.2
                .addColumn("groundDefenseEquipmentType3") // 地导装备类型.3
                .addColumn("groundDefenseEquipmentId3")   // 地导装备ID.3
                .addColumn("jammingEffective")            // 干扰是否有效
                .addColumn("jamming")                     // 干扰
                .addColumn("afterburner")               // 加力
                .addColumn("headOn")                    // 迎头
                .addColumn("heading")                   // 航向
                .addColumn("pitch")                     // 俯仰
                .setUseHeader(true)
                .setColumnSeparator(',')
                .setLineSeparator("\n")
                .build();
    }

    @Override
    protected String getInsertQuery() {
        return """
                INSERT INTO `rtkn` (
                    sortie_number, aircraft_id, message_time, satellite_guidance_time, local_time, message_sequence_number, weapon_id, weapon_type, target_id, intercepted_weapon_id, 
                    hit_result, miss_reason, miss_distance, matching_failure_reason, ground_defense_equipment_type, ground_defense_equipment_id, ground_defense_equipment_type1, ground_defense_equipment_id1, 
                    ground_defense_equipment_type2, ground_defense_equipment_id2, ground_defense_equipment_type3, ground_defense_equipment_id3, jamming_effective, jamming, afterburner, head_on, heading, pitch
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                    ?, ?, ?, ?, ?, ?, ?
                );
                """;
    }

    @Override
    protected void bindPreparedStatement(PreparedStatement preparedStatement, Rtkn data, String sortieNumber) throws SQLException {
// 注意 sortieNumber 是从配置里面来的 csv里面没有
        preparedStatement.setString(1, sortieNumber);
        preparedStatement.setString(2, data.getAircraftId());
        // LocalTime -> java.sql.Time
        preparedStatement.setTime(3, data.getMessageTime() != null ? Time.valueOf(data.getMessageTime()) : null);
        preparedStatement.setTime(4, data.getSatelliteGuidanceTime() != null ? Time.valueOf(data.getSatelliteGuidanceTime()) : null);
        preparedStatement.setTime(5, data.getLocalTime() != null ? Time.valueOf(data.getLocalTime()) : null);
        // Handle potential null for Long
        if (data.getMessageSequenceNumber() != null) {
            preparedStatement.setLong(6, data.getMessageSequenceNumber());
        } else {
            preparedStatement.setNull(6, java.sql.Types.BIGINT);
        }
        preparedStatement.setString(7, data.getWeaponId());
        preparedStatement.setString(8, data.getWeaponType());
        preparedStatement.setString(9, data.getTargetId());
        preparedStatement.setString(10, data.getInterceptedWeaponId());
        preparedStatement.setString(11, data.getHitResult());
        preparedStatement.setString(12, data.getMissReason());
        preparedStatement.setString(13, data.getMissDistance());
        preparedStatement.setString(14, data.getMatchingFailureReason());
        preparedStatement.setString(15, data.getGroundDefenseEquipmentType());
        preparedStatement.setString(16, data.getGroundDefenseEquipmentId());
        preparedStatement.setString(17, data.getGroundDefenseEquipmentType1());
        preparedStatement.setString(18, data.getGroundDefenseEquipmentId1());
        preparedStatement.setString(19, data.getGroundDefenseEquipmentType2());
        preparedStatement.setString(20, data.getGroundDefenseEquipmentId2());
        preparedStatement.setString(21, data.getGroundDefenseEquipmentType3());
        preparedStatement.setString(22, data.getGroundDefenseEquipmentId3());
        preparedStatement.setString(23, data.getJammingEffective());
        preparedStatement.setString(24, data.getJamming());
        preparedStatement.setString(25, data.getAfterburner());
        preparedStatement.setString(26, data.getHeadOn());
        preparedStatement.setString(27, data.getHeading());
        preparedStatement.setString(28, data.getPitch());
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
                parameterTool.getRequired(SIMULATION_SORTIE_NUMBER.getKeyForParamsMap()));
        RtknFileReceiver receiver = new RtknFileReceiver();
        receiver.setConfig(config);
        receiver.run();
    }
}
