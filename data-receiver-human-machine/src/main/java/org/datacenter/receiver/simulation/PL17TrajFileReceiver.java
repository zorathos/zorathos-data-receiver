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
import org.datacenter.model.simulation.PL17Traj;
import org.datacenter.receiver.simulation.base.SimulationReceiver;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;


/**
 * @author : [宁]
 * @description : 仿真数据PL17Traj的接收器
 */
@Slf4j
@Data
@EqualsAndHashCode(callSuper = true)
public class PL17TrajFileReceiver extends SimulationReceiver<PL17Traj> {

    @Override
    public void prepare() {
        table = TiDBTable.PL17_TRAJ;
        modelClass = PL17Traj.class;
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
                .addColumn("pylonId")                 // 挂架ID
                .addColumn("weaponType")              // 武器类型
                .addColumn("targetId")                // 目标ID
                .addColumn("longitude")               // 经度
                .addColumn("latitude")                // 纬度
                .addColumn("altitude")                // 高度
                .addColumn("missileTargetDistance")   // 弹目距离
                .addColumn("missileSpeed")            // 弹速度(m/s)
                .addColumn("interceptionStatus")      // 截获状态
                .addColumn("nonInterceptionReason")   // 未截获原因
                .addColumn("seekerAzimuth")           // 导引头视线方位角
                .addColumn("seekerElevation")         // 导引头视线俯仰角
                .addColumn("targetTspiStatus")        // 目标TSPI状态
                .addColumn("commandMachineStatus")    // 指令机状态
                .addColumn("groundAngleSatisfactionFlag") // 擦地角满足标志
                .addColumn("zeroCrossingFlag")        // 过零标志
                .addColumn("distanceInterceptionFlag") // 距离截获标志
                .addColumn("speedInterceptionFlag")    // 速度截获标志
                .addColumn("angleInterceptionFlag")    // 角度截获标志
                .setUseHeader(true)
                .setColumnSeparator(',')
                .setLineSeparator("\n")
                .build();
    }

    @Override
    protected String getInsertQuery() {
        return """
                INSERT INTO `pl17_traj` (
                    sortie_number, aircraft_id, message_time, satellite_guidance_time, local_time, message_sequence_number, weapon_id, pylon_id, weapon_type, target_id, 
                    longitude, latitude, altitude, missile_target_distance, missile_speed, interception_status, non_interception_reason, seeker_azimuth, seeker_elevation, 
                    target_tspi_status, command_machine_status, ground_angle_satisfaction_flag, zero_crossing_flag, distance_interception_flag, speed_interception_flag, angle_interception_flag
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                    ?, ?, ?, ?, ?, ?
                );
                """;
    }

    @Override
    protected void bindPreparedStatement(PreparedStatement preparedStatement, PL17Traj data, String sortieNumber) throws SQLException {
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
        preparedStatement.setString(8, data.getPylonId());
        preparedStatement.setString(9, data.getWeaponType());
        preparedStatement.setString(10, data.getTargetId());
        preparedStatement.setString(11, data.getLongitude());
        preparedStatement.setString(12, data.getLatitude());
        preparedStatement.setString(13, data.getAltitude());
        preparedStatement.setString(14, data.getMissileTargetDistance());
        preparedStatement.setString(15, data.getMissileSpeed());
        preparedStatement.setString(16, data.getInterceptionStatus());
        preparedStatement.setString(17, data.getNonInterceptionReason());
        preparedStatement.setString(18, data.getSeekerAzimuth());
        preparedStatement.setString(19, data.getSeekerElevation());
        preparedStatement.setString(20, data.getTargetTspiStatus());
        preparedStatement.setString(21, data.getCommandMachineStatus());
        preparedStatement.setString(22, data.getGroundAngleSatisfactionFlag());
        preparedStatement.setString(23, data.getZeroCrossingFlag());
        preparedStatement.setString(24, data.getDistanceInterceptionFlag());
        preparedStatement.setString(25, data.getSpeedInterceptionFlag());
        preparedStatement.setString(26, data.getAngleInterceptionFlag());
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
        PL17TrajFileReceiver receiver = new PL17TrajFileReceiver();
        receiver.setConfig(config);
        receiver.run();
    }
}
