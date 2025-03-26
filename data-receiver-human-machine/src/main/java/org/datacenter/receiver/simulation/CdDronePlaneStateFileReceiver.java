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
import org.datacenter.model.simulation.CdDronePlaneState;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;


/**
 * @author : [宁]
 * @description : 仿真数据CdDronePlaneState的接收器
 */
@Slf4j
@Data
@EqualsAndHashCode(callSuper = true)
public class CdDronePlaneStateFileReceiver extends SimulationBaseReceiver<CdDronePlaneState> {

    @Override
    public void prepare() {
        table = TiDBTable.CD_DRONE_PLANE_STATE;
        modelClass = CdDronePlaneState.class;
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
                .addColumn("trueAngleOfAttack")       // 真攻角
                .addColumn("machNumber")              // 马赫数
                .addColumn("normalLoadFactor")        // 法向过载
                .addColumn("indicatedAirspeed")       // 表速(km/h)
                .addColumn("fieldElevation")          // 场高
                .addColumn("radioAltitude")           // 无线电高度
                .addColumn("remainingFuel")           // 余油量
                .addColumn("manualRespawn")           // 手动复活
                .addColumn("parameterSettingStatus")  // 参数设置状态
                .setUseHeader(true)
                .setColumnSeparator(',')
                .setLineSeparator("\n")
                .build();
    }

    @Override
    protected String getInsertQuery() {
        return """
INSERT INTO `cd_drone_plane_state` (
    sortie_number, aircraft_id, message_time, satellite_guidance_time, local_time, message_sequence_number, true_angle_of_attack, mach_number, normal_load_factor, indicated_airspeed, 
    field_elevation, radio_altitude, remaining_fuel, manual_respawn, parameter_setting_status
) VALUES (
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
    ?, ?, ?, ?, ?
);
""";
    }

    @Override
    protected void bindPreparedStatement(PreparedStatement preparedStatement, CdDronePlaneState data, String sortieNumber) throws SQLException {
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
        preparedStatement.setString(7, data.getTrueAngleOfAttack());
        preparedStatement.setString(8, data.getMachNumber());
        preparedStatement.setString(9, data.getNormalLoadFactor());
        preparedStatement.setString(10, data.getIndicatedAirspeed());
        preparedStatement.setString(11, data.getFieldElevation());
        preparedStatement.setString(12, data.getRadioAltitude());
        preparedStatement.setString(13, data.getRemainingFuel());
        preparedStatement.setString(14, data.getManualRespawn());
        preparedStatement.setString(15, data.getParameterSettingStatus());
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
        CdDronePlaneStateFileReceiver receiver = new CdDronePlaneStateFileReceiver();
        receiver.setConfig(config);
        receiver.run();
    }
}