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
import org.datacenter.model.simulation.IrMsl;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;


/**
 * @author : [宁]
 * @description : 仿真数据IrMsl的接收器
 */
@Slf4j
@Data
@EqualsAndHashCode(callSuper = true)
public class IrMslFileReceiver extends SimulationBaseReceiver<IrMsl> {

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
                    sortie_number, aircraft_id, message_time, satellite_guidance_time, local_time, message_sequence_number, seeker_azimuth, seeker_elevation, weapon_type, interception_flag
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                );
                """;
    }

    @Override
    protected void bindPreparedStatement(PreparedStatement preparedStatement, IrMsl data, String sortieNumber) throws SQLException {
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
        preparedStatement.setString(7, data.getSeekerAzimuth());
        preparedStatement.setString(8, data.getSeekerElevation());
        preparedStatement.setString(9, data.getWeaponType());
        preparedStatement.setString(10, data.getInterceptionFlag());
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
        IrMslFileReceiver receiver = new IrMslFileReceiver();
        receiver.setConfig(config);
        receiver.run();
    }
}
