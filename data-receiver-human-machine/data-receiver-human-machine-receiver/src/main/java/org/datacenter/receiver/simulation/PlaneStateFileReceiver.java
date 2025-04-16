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
import org.datacenter.model.simulation.PlaneState;
import org.datacenter.receiver.simulation.base.SimulationReceiver;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;

import static org.datacenter.config.keys.HumanMachineReceiverConfigKey.SIMULATION_SORTIE_NUMBER;
import static org.datacenter.config.keys.HumanMachineReceiverConfigKey.SIMULATION_URL;


/**
 * @author : [宁]
 * @description : 仿真数据PlaneState的接收器
 */
@Slf4j
@Data
@EqualsAndHashCode(callSuper = true)
public class PlaneStateFileReceiver extends SimulationReceiver<PlaneState> {

    @Override
    public void prepare() {
        table = TiDBTable.PLANE_STATE;
        modelClass = PlaneState.class;
        super.prepare();
    }

    @Override
    protected SerializableFunction<CsvMapper, CsvSchema> getSchemaGenerator() {
        return mapper -> CsvSchema.builder()
                .addColumn("aircraftId")           // 飞机ID
                .addColumn("messageTime")          // 消息时间
                .addColumn("satelliteGuidanceTime")// 卫导时间
                .addColumn("localTime")            // 本地时间
                .addColumn("messageSequenceNumber")// 消息序列号
                .addColumn("trueAngleOfAttack")    // 真攻角
                .addColumn("machNumber")           // 马赫数
                .addColumn("normalLoadFactor")     // 法向过载
                .addColumn("indicatedAirspeed")    // 表速(km/h)
                .addColumn("fieldElevation")       // 场高
                .addColumn("radioAltitude")        // 无线电高度
                .addColumn("remainingFuel")        // 余油量
                .addColumn("scenario")             // 场景
                .addColumn("manualRespawn")        // 手动复活
                .addColumn("parameterSettingStatus") // 参数设置状态
                .addColumn("encryptionStatus")     // 加密状态
                .setUseHeader(true)
                .setColumnSeparator(',')
                .setLineSeparator("\n")
                .build();
    }

    @Override
    protected String getInsertQuery() {
        return """
                INSERT INTO `plane_state` (
                    sortie_number, aircraft_id, message_time, satellite_guidance_time, local_time, message_sequence_number, true_angle_of_attack, mach_number, normal_load_factor, indicated_airspeed, 
                    field_elevation, radio_altitude, remaining_fuel, scenario, manual_respawn, parameter_setting_status, encryption_status
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                    ?, ?, ?, ?, ?, ?, ?
                );
                """;
    }

    @Override
    protected void bindPreparedStatement(PreparedStatement preparedStatement, PlaneState data, String sortieNumber) throws SQLException {
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
        preparedStatement.setString(14, data.getScenario());
        preparedStatement.setString(15, data.getManualRespawn());
        preparedStatement.setString(16, data.getParameterSettingStatus());
        preparedStatement.setString(17, data.getEncryptionStatus());
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
        PlaneStateFileReceiver receiver = new PlaneStateFileReceiver();
        receiver.setConfig(config);
        receiver.run();
    }
}
