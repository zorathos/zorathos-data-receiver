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
import org.datacenter.model.simulation.IrMsl;
import org.datacenter.receiver.simulation.base.SimulationReceiver;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;

import static org.datacenter.config.keys.HumanMachineReceiverConfigKey.IMPORT_ID;
import static org.datacenter.config.keys.HumanMachineReceiverConfigKey.SIMULATION_BATCH_NUMBER;
import static org.datacenter.config.keys.HumanMachineReceiverConfigKey.SIMULATION_URL;


/**
 * @author : [宁]
 * @description : 仿真数据IrMsl的接收器
 */
@Slf4j
@Data
@EqualsAndHashCode(callSuper = true)
public class IrMslFileReceiver extends SimulationReceiver<IrMsl> {

    @Override
    public void prepare() {
        table = TiDBTable.IR_MSL;
        modelClass = IrMsl.class;
        super.prepare();
    }

    @Override
    protected SerializableFunction<CsvMapper, CsvSchema> getSchemaGenerator() {
        return mapper -> CsvSchema.builder()
                .addColumn("aircraftId")            // 飞机ID
                .addColumn("messageTime")           // 消息时间
                .addColumn("satelliteGuidanceTime") // 卫导时间
                .addColumn("localTime")             // 本地时间
                .addColumn("messageSequenceNumber") // 消息序列号
                .addColumn("seekerAzimuth")         // 导引头方位角
                .addColumn("seekerElevation")       // 导引头俯仰角
                .addColumn("weaponType")            // 武器类型
                .addColumn("interceptionFlag")      // 截获标识
                .setUseHeader(true)
                .setColumnSeparator(',')
                .setLineSeparator("\n")
                .build();
    }

    @Override
    protected String getInsertQuery() {
        return """
                INSERT INTO `ir_msl` (
                    import_id,batch_number, aircraft_id, message_time, satellite_guidance_time, local_time, message_sequence_number, seeker_azimuth, seeker_elevation, weapon_type, interception_flag
                ) VALUES (
                    ?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                );
                """;
    }

    @Override
    protected void bindPreparedStatement(PreparedStatement preparedStatement, IrMsl data, String batchNumber, long importId) throws SQLException {
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
        preparedStatement.setString(8, data.getSeekerAzimuth());
        preparedStatement.setString(9, data.getSeekerElevation());
        preparedStatement.setString(10, data.getWeaponType());
        preparedStatement.setString(11, data.getInterceptionFlag());
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
        IrMslFileReceiver receiver = new IrMslFileReceiver();
        receiver.setConfig(config);
        receiver.run();
    }
}
