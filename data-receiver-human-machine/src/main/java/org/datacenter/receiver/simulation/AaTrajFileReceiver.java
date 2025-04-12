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
import org.datacenter.model.simulation.AaTraj;
import org.datacenter.receiver.simulation.base.SimulationReceiver;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;


/**
 * @author : [wangminan]
 * @description : 仿真数据AaTraj的接收器
 */
@Slf4j
@Data
@EqualsAndHashCode(callSuper = true)
public class AaTrajFileReceiver extends SimulationReceiver<AaTraj> {

    @Override
    public void prepare() {
        table = TiDBTable.AA_TRAJ;
        modelClass = AaTraj.class;
        super.prepare();
    }

    @Override
    protected SerializableFunction<CsvMapper, CsvSchema> getSchemaGenerator() {
        return mapper -> CsvSchema.builder()
                .addColumn("aircraftId")
                .addColumn("messageTime")
                .addColumn("satelliteGuidanceTime")
                .addColumn("localTime")
                .addColumn("messageSequenceNumber")
                .addColumn("weaponId")
                .addColumn("pylonId")
                .addColumn("weaponType")
                .addColumn("targetId")
                .addColumn("longitude")
                .addColumn("latitude")
                .addColumn("altitude")
                .addColumn("missileTargetDistance")
                .addColumn("missileSpeed")
                .addColumn("interceptionStatus")
                .addColumn("nonInterceptionReason")
                .addColumn("seekerAzimuth")
                .addColumn("seekerElevation")
                .addColumn("targetTspiStatus")
                .addColumn("commandMachineStatus")
                .addColumn("groundAngleSatisfactionFlag")
                .addColumn("zeroCrossingFlag")
                .setUseHeader(true)
                .setColumnSeparator(',')
                .setLineSeparator("\n")
                .build();
    }

    @Override
    protected String getInsertQuery() {
        return """
                INSERT INTO `aa_traj` (
                    sortie_number, aircraft_id, message_time, satellite_guidance_time, local_time, message_sequence_number, weapon_id, pylon_id, weapon_type, target_id, 
                    longitude, latitude, altitude, missile_target_distance, missile_speed, interception_status, non_interception_reason, seeker_azimuth, seeker_elevation, 
                    target_tspi_status, command_machine_status, ground_angle_satisfaction_flag, zero_crossing_flag
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                );
                """;
    }

    @Override
    protected void bindPreparedStatement(PreparedStatement preparedStatement, AaTraj data, String sortieNumber) throws SQLException {
        // 字符串转localDate sortieNumber.split("_")[0]
//        LocalDate localDate = LocalDate.parse(sortieNumber.split("_")[0], DateTimeFormatter.ofPattern("yyyyMMdd"));
        // 注意 sortieNumber 是从配置里面来的 csv里面没有
        preparedStatement.setString(1, sortieNumber);
        preparedStatement.setString(2, data.getAircraftId());
        // LocalTime解析不了 全部用Unix时间戳 Long型
        preparedStatement.setTime(3, Time.valueOf(data.getMessageTime()));
        preparedStatement.setTime(4, Time.valueOf(data.getSatelliteGuidanceTime()));
        preparedStatement.setTime(5, Time.valueOf(data.getLocalTime()));
        preparedStatement.setLong(6, data.getMessageSequenceNumber());
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
        AaTrajFileReceiver receiver = new AaTrajFileReceiver();
        receiver.setConfig(config);
        receiver.run();
    }
}
