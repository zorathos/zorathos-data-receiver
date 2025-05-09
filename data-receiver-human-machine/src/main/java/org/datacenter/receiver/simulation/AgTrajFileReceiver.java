package org.datacenter.receiver.simulation;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.flink.util.function.SerializableFunction;
import org.datacenter.config.receiver.simulation.SimulationReceiverConfig;
import org.datacenter.model.base.TiDBTable;
import org.datacenter.model.simulation.AgTraj;
import org.datacenter.receiver.simulation.base.SimulationReceiver;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;

import static org.datacenter.config.keys.HumanMachineReceiverConfigKey.SIMULATION_SORTIE_NUMBER;
import static org.datacenter.config.keys.HumanMachineReceiverConfigKey.SIMULATION_URL;

public class AgTrajFileReceiver extends SimulationReceiver<AgTraj> {
    @Override
    public void prepare() {
        table = TiDBTable.AG_TRAJ;
        modelClass = AgTraj.class;
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
                .addColumn("weaponId")                // 武器ID
                .addColumn("weaponType")              // 武器类型
                .addColumn("longitude")               // 经度
                .addColumn("latitude")                // 纬度
                .addColumn("altitude")                // 高度
                .addColumn("heading")                 // 航向
                .addColumn("pitch")                   // 俯仰
                .addColumn("northSpeed")              // 北速
                .addColumn("skySpeed")                // 天速  (Note: Java field is skySpeed, CSV header is 天速)
                .addColumn("eastSpeed")               // 东速
                .addColumn("seekerId")                // 导引头号
                .addColumn("interceptionFlag")        // 截获标志
                .addColumn("terminationFlag")         // 终止标志
                .addColumn("interceptingMemberId")    // 截获成员ID
                .addColumn("interceptingEquipmentId") // 截获装备ID
                .addColumn("interceptingEquipmentType")// 截获装备类型
                .addColumn("launcherId")              // 发射方ID
                .addColumn("seekerAzimuthCenter")     // 导引头方位中心
                .addColumn("seekerPitchCenter")       // 导引头俯仰中心
                .addColumn("targetId")                // 目标ID
                .addColumn("missileTargetDistance")   // 弹目距离
                .setUseHeader(true)
                .setColumnSeparator(',')
                .setLineSeparator("\n")
                .build();
    }

    @Override
    protected String getInsertQuery() {
        return """
                INSERT INTO `ag_traj` (
                    sortie_number, aircraft_id, message_time, satellite_guidance_time, local_time, message_sequence_number, weapon_id, weapon_type, longitude, latitude,
                    altitude, heading, pitch, north_speed, sky_speed, east_speed, seeker_id, interception_flag, termination_flag, intercepting_member_id,
                    intercepting_equipment_id, intercepting_equipment_type, launcher_id, seeker_azimuth_center, seeker_pitch_center, target_id, missile_target_distance
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?
                );
                """;
    }

    @Override
    protected void bindPreparedStatement(PreparedStatement preparedStatement, AgTraj data, String sortieNumber) throws SQLException {
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
        preparedStatement.setString(9, data.getLongitude());
        preparedStatement.setString(10, data.getLatitude());
        preparedStatement.setString(11, data.getAltitude());
        preparedStatement.setString(12, data.getHeading());
        preparedStatement.setString(13, data.getPitch());
        preparedStatement.setString(14, data.getNorthSpeed());
        preparedStatement.setString(15, data.getSkySpeed()); // Corresponds to 天速
        preparedStatement.setString(16, data.getEastSpeed());
        preparedStatement.setString(17, data.getSeekerId());
        preparedStatement.setString(18, data.getInterceptionFlag());
        preparedStatement.setString(19, data.getTerminationFlag());
        preparedStatement.setString(20, data.getInterceptingMemberId());
        preparedStatement.setString(21, data.getInterceptingEquipmentId());
        preparedStatement.setString(22, data.getInterceptingEquipmentType());
        preparedStatement.setString(23, data.getLauncherId());
        preparedStatement.setString(24, data.getSeekerAzimuthCenter());
        preparedStatement.setString(25, data.getSeekerPitchCenter());
        preparedStatement.setString(26, data.getTargetId());
        preparedStatement.setString(27, data.getMissileTargetDistance());
    }

    public static void main(String[] args) {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        SimulationReceiverConfig config = new SimulationReceiverConfig(
                parameterTool.getRequired(SIMULATION_URL.getKeyForParamsMap()),
                parameterTool.getRequired(SIMULATION_SORTIE_NUMBER.getKeyForParamsMap()));
        AgTrajFileReceiver receiver = new AgTrajFileReceiver();
        receiver.setConfig(config);
        receiver.run();
    }
}
