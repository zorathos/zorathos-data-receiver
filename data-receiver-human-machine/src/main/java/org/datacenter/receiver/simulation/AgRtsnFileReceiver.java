package org.datacenter.receiver.simulation;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.flink.util.function.SerializableFunction;
import org.datacenter.config.simulation.SimulationReceiverConfig;
import org.datacenter.model.base.TiDBTable;
import org.datacenter.model.simulation.AgRtsn;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;

public class AgRtsnFileReceiver extends SimulationBaseReceiver<AgRtsn> {
    @Override
    public void prepare() {
        table = TiDBTable.AG_RTSN;
        modelClass = AgRtsn.class;
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
                .addColumn("weaponPylonId")           // 武器挂架ID
                .addColumn("weaponType")              // 武器类型
                .addColumn("numberOfMissilesReleased")// 投放弹数
                .addColumn("aircraftGroundSpeed")     // 载机地速
                .addColumn("aircraftLongitude")       // 载机经度
                .addColumn("aircraftLatitude")        // 载机纬度
                .addColumn("aircraftAltitude")        // 载机高度
                .addColumn("aircraftHeading")         // 载机航向
                .addColumn("aircraftPitch")           // 载机俯仰
                .addColumn("aircraftRoll")            // 载机横滚
                .addColumn("aircraftAngleOfAttack")   // 载机攻角
                .addColumn("aircraftNorthSpeed")      // 载机北速
                .addColumn("aircraftVerticalSpeed")   // 载机天速
                .addColumn("aircraftEastSpeed")       // 载机东速
                .addColumn("northWindSpeed")          // 北向风速
                .addColumn("verticalWindSpeed")       // 天向风速
                .addColumn("eastWindSpeed")           // 东向风速
                .addColumn("targetLongitude")         // 目标经度
                .addColumn("targetLatitude")          // 目标纬度
                .addColumn("targetAltitude")          // 目标高度
                .addColumn("targetDistance")          // 目标距离
                .addColumn("seekerHeadNumber")        // 导引头号
                .addColumn("targetCoordinateValidity") // 目标经纬高有效标识
                .addColumn("targetAzimuthElevationValidity") // 目标方位俯仰有效标识
                .addColumn("targetElevationAngle")    // 目标俯仰角(惯性侧滑角)
                .addColumn("targetAzimuthAngle")      // 目标方位角(真空速)
                .addColumn("impactAngleValidity")     // 落角有效性
                .addColumn("entryAngle")              // 进入角
                .addColumn("impactAngle")             // 落角
                .addColumn("directionValidity")       // 方向有效性
                .setUseHeader(true)
                .setColumnSeparator(',')
                .setLineSeparator("\n")
                .build();
    }

    @Override
    protected String getInsertQuery() {
        return """
                INSERT INTO `ag_rtsn` (
                    sortie_number, aircraft_id, message_time, satellite_guidance_time, local_time, message_sequence_number, weapon_id, weapon_pylon_id, weapon_type, number_of_missiles_released, 
                    aircraft_ground_speed, aircraft_longitude, aircraft_latitude, aircraft_altitude, aircraft_heading, aircraft_pitch, aircraft_roll, aircraft_angle_of_attack, aircraft_north_speed, 
                    aircraft_vertical_speed, aircraft_east_speed, north_wind_speed, vertical_wind_speed, east_wind_speed, target_longitude, target_latitude, target_altitude, target_distance, 
                    seeker_head_number, target_coordinate_validity, target_azimuth_elevation_validity, target_elevation_angle, target_azimuth_angle, impact_angle_validity, entry_angle, 
                    impact_angle, direction_validity
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                    ?, ?, ?, ?, ?, ?, ?
                );
                """;
    }

    @Override
    protected void bindPreparedStatement(PreparedStatement preparedStatement, AgRtsn data, String sortieNumber) throws SQLException {
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
        preparedStatement.setString(8, data.getWeaponPylonId());
        preparedStatement.setString(9, data.getWeaponType());
        preparedStatement.setString(10, data.getNumberOfMissilesReleased());
        preparedStatement.setString(11, data.getAircraftGroundSpeed());
        preparedStatement.setString(12, data.getAircraftLongitude());
        preparedStatement.setString(13, data.getAircraftLatitude());
        preparedStatement.setString(14, data.getAircraftAltitude());
        preparedStatement.setString(15, data.getAircraftHeading());
        preparedStatement.setString(16, data.getAircraftPitch());
        preparedStatement.setString(17, data.getAircraftRoll());
        preparedStatement.setString(18, data.getAircraftAngleOfAttack());
        preparedStatement.setString(19, data.getAircraftNorthSpeed());
        preparedStatement.setString(20, data.getAircraftVerticalSpeed());
        preparedStatement.setString(21, data.getAircraftEastSpeed());
        preparedStatement.setString(22, data.getNorthWindSpeed());
        preparedStatement.setString(23, data.getVerticalWindSpeed());
        preparedStatement.setString(24, data.getEastWindSpeed());
        preparedStatement.setString(25, data.getTargetLongitude());
        preparedStatement.setString(26, data.getTargetLatitude());
        preparedStatement.setString(27, data.getTargetAltitude());
        preparedStatement.setString(28, data.getTargetDistance());
        preparedStatement.setString(29, data.getSeekerHeadNumber());
        preparedStatement.setString(30, data.getTargetCoordinateValidity());
        preparedStatement.setString(31, data.getTargetAzimuthElevationValidity());
        preparedStatement.setString(32, data.getTargetElevationAngle());
        preparedStatement.setString(33, data.getTargetAzimuthAngle());
        preparedStatement.setString(34, data.getImpactAngleValidity());
        preparedStatement.setString(35, data.getEntryAngle());
        preparedStatement.setString(36, data.getImpactAngle());
        preparedStatement.setString(37, data.getDirectionValidity());
    }

    public static void main(String[] args) {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        SimulationReceiverConfig config = new SimulationReceiverConfig(
                parameterTool.getRequired("url"),
                parameterTool.getRequired("sortie_number"));
        AgRtsnFileReceiver receiver = new AgRtsnFileReceiver();
        receiver.setConfig(config);
        receiver.run();
    }
}
