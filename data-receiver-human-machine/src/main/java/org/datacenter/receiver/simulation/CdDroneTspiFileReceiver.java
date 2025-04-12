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
import org.datacenter.model.simulation.CdDroneTspi;
import org.datacenter.receiver.simulation.base.SimulationReceiver;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;


/**
 * @author : [宁]
 * @description : 仿真数据CdDroneTspi的接收器
 */
@Slf4j
@Data
@EqualsAndHashCode(callSuper = true)
public class CdDroneTspiFileReceiver extends SimulationReceiver<CdDroneTspi> {

    @Override
    public void prepare() {
        table = TiDBTable.CD_DRONE_TSPI;
        modelClass = CdDroneTspi.class;
        super.prepare();
    }

    @Override
    protected SerializableFunction<CsvMapper, CsvSchema> getSchemaGenerator() {
        return mapper -> CsvSchema.builder()
                .addColumn("aircraftId")              // 飞机ID
                .addColumn("aircraftType")            // 飞机类型
                .addColumn("messageTime")             // 消息时间
                .addColumn("satelliteGuidanceTime")   // 卫导时间
                .addColumn("localTime")               // 本地时间
                .addColumn("messageSequenceNumber")   // 消息序列号
                .addColumn("longitude")               // 经度
                .addColumn("latitude")                // 纬度
                .addColumn("pressureAltitude")        // 气压高度
                .addColumn("roll")                    // 横滚
                .addColumn("pitch")                   // 俯仰
                .addColumn("heading")                 // 航向
                .addColumn("satelliteAltitude")       // 卫星高度
                .addColumn("trainingStatus")          // 训练状态
                .addColumn("chaff")                   // 干扰弹
                .addColumn("afterburner")             // 加力
                .addColumn("northVelocity")           // 北向速度
                .addColumn("verticalVelocity")        // 天向速度
                .addColumn("eastVelocity")            // 东向速度
                .addColumn("delayStatus")             // 延迟状态
                .setUseHeader(true)
                .setColumnSeparator(',')
                .setLineSeparator("\n")
                .build();
    }

    @Override
    protected String getInsertQuery() {
        return """
                INSERT INTO `cd_drone_tspi` (
                    sortie_number, aircraft_id, aircraft_type, message_time, satellite_guidance_time, local_time, message_sequence_number, longitude, latitude, pressure_altitude, 
                    roll, pitch, heading, satellite_altitude, training_status, chaff, afterburner, north_velocity, vertical_velocity, east_velocity, 
                    delay_status
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                    ?
                );
                """;
    }

    @Override
    protected void bindPreparedStatement(PreparedStatement preparedStatement, CdDroneTspi data, String sortieNumber) throws SQLException {
        // 注意 sortieNumber 是从配置里面来的 csv里面没有
        preparedStatement.setString(1, sortieNumber);
        preparedStatement.setString(2, data.getAircraftId());
        preparedStatement.setString(3, data.getAircraftType());
        // LocalTime -> java.sql.Time
        preparedStatement.setTime(4, data.getMessageTime() != null ? Time.valueOf(data.getMessageTime()) : null);
        preparedStatement.setTime(5, data.getSatelliteGuidanceTime() != null ? Time.valueOf(data.getSatelliteGuidanceTime()) : null);
        preparedStatement.setTime(6, data.getLocalTime() != null ? Time.valueOf(data.getLocalTime()) : null);
        // Handle potential null for Long
        if (data.getMessageSequenceNumber() != null) {
            preparedStatement.setLong(7, data.getMessageSequenceNumber());
        } else {
            preparedStatement.setNull(7, java.sql.Types.BIGINT);
        }
        preparedStatement.setString(8, data.getLongitude());
        preparedStatement.setString(9, data.getLatitude());
        preparedStatement.setString(10, data.getPressureAltitude());
        preparedStatement.setString(11, data.getRoll());
        preparedStatement.setString(12, data.getPitch());
        preparedStatement.setString(13, data.getHeading());
        preparedStatement.setString(14, data.getSatelliteAltitude());
        preparedStatement.setString(15, data.getTrainingStatus());
        preparedStatement.setString(16, data.getChaff());
        preparedStatement.setString(17, data.getAfterburner());
        preparedStatement.setString(18, data.getNorthVelocity());
        preparedStatement.setString(19, data.getVerticalVelocity());
        preparedStatement.setString(20, data.getEastVelocity());
        preparedStatement.setString(21, data.getDelayStatus());
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
        CdDroneTspiFileReceiver receiver = new CdDroneTspiFileReceiver();
        receiver.setConfig(config);
        receiver.run();
    }
}
