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
import org.datacenter.model.simulation.Tgt;
import org.datacenter.receiver.simulation.base.SimulationReceiver;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;

import static org.datacenter.config.keys.HumanMachineReceiverConfigKey.IMPORT_ID;
import static org.datacenter.config.keys.HumanMachineReceiverConfigKey.SIMULATION_BATCH_NUMBER;
import static org.datacenter.config.keys.HumanMachineReceiverConfigKey.SIMULATION_URL;

/**
 * @author : [宁]
 * @description : 仿真数据Tgt的接收器
 */
@Slf4j
@Data
@EqualsAndHashCode(callSuper = true)
public class TgtFileReceiver extends SimulationReceiver<Tgt> {

    @Override
    public void prepare() {
        table = TiDBTable.TGT;
        modelClass = Tgt.class;
        super.prepare();
    }

    @Override
    protected SerializableFunction<CsvMapper, CsvSchema> getSchemaGenerator() {
        return mapper -> CsvSchema.builder()
                .addColumn("aircraftId")          // 飞机ID
                .addColumn("messageTime")         // 消息时间
                .addColumn("satelliteGuidanceTime")  // 卫导时间
                .addColumn("localTime")           // 本地时间
                .addColumn("messageSequenceNumber") // 消息序列号
                .addColumn("targetCount")         // 目标数量
                .addColumn("identifier1")         // 标识1
                .addColumn("sensor1")             // 传感器1
                .addColumn("pitch1")              // 俯仰1
                .addColumn("azimuth1")            // 方位1
                .addColumn("slantRange1")         // 斜距1
                .addColumn("identifier2")         // 标识2
                .addColumn("sensor2")             // 传感器2
                .addColumn("pitch2")              // 俯仰2
                .addColumn("azimuth2")            // 方位2
                .addColumn("slantRange2")         // 斜距2
                .addColumn("identifier3")         // 标识3
                .addColumn("sensor3")             // 传感器3
                .addColumn("pitch3")              // 俯仰3
                .addColumn("azimuth3")            // 方位3
                .addColumn("slantRange3")         // 斜距3
                .addColumn("identifier4")         // 标识4
                .addColumn("sensor4")             // 传感器4
                .addColumn("pitch4")              // 俯仰4
                .addColumn("azimuth4")            // 方位4
                .addColumn("slantRange4")         // 斜距4
                .addColumn("identifier5")         // 标识5
                .addColumn("sensor5")             // 传感器5
                .addColumn("pitch5")              // 俯仰5
                .addColumn("azimuth5")            // 方位5
                .addColumn("slantRange5")         // 斜距5
                .addColumn("identifier6")         // 标识6
                .addColumn("sensor6")             // 传感器6
                .addColumn("pitch6")              // 俯仰6
                .addColumn("azimuth6")            // 方位6
                .addColumn("slantRange6")         // 斜距6
                .addColumn("identifier7")         // 标识7
                .addColumn("sensor7")             // 传感器7
                .addColumn("pitch7")              // 俯仰7
                .addColumn("azimuth7")            // 方位7
                .addColumn("slantRange7")         // 斜距7
                .addColumn("identifier8")         // 标识8
                .addColumn("sensor8")             // 传感器8
                .addColumn("pitch8")              // 俯仰8
                .addColumn("azimuth8")            // 方位8
                .addColumn("slantRange8")         // 斜距8
                .setUseHeader(true)
                .setColumnSeparator(',')
                .setLineSeparator("\n")
                .build();
    }

    @Override
    protected String getInsertQuery() {
        return """
                INSERT INTO `tgt` (
                    import_id,batch_number, aircraft_id, message_time, satellite_guidance_time, local_time, message_sequence_number, target_count, identifier1, sensor1, pitch1, azimuth1, slant_range1, 
                    identifier2, sensor2, pitch2, azimuth2, slant_range2, identifier3, sensor3, pitch3, azimuth3, slant_range3, identifier4, sensor4, pitch4, azimuth4, slant_range4, 
                    identifier5, sensor5, pitch5, azimuth5, slant_range5, identifier6, sensor6, pitch6, azimuth6, slant_range6, identifier7, sensor7, pitch7, azimuth7, slant_range7, 
                    identifier8, sensor8, pitch8, azimuth8, slant_range8
                ) VALUES (
                    ?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                    ?, ?, ?, ?, ?
                );
                """;
    }

    @Override
    protected void bindPreparedStatement(PreparedStatement preparedStatement, Tgt data, String batchNumber, long importId) throws SQLException {
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
        preparedStatement.setString(8, data.getTargetCount());
        preparedStatement.setString(9, data.getIdentifier1());
        preparedStatement.setString(10, data.getSensor1());
        preparedStatement.setString(11, data.getPitch1());
        preparedStatement.setString(12, data.getAzimuth1());
        preparedStatement.setString(13, data.getSlantRange1());
        preparedStatement.setString(14, data.getIdentifier2());
        preparedStatement.setString(15, data.getSensor2());
        preparedStatement.setString(16, data.getPitch2());
        preparedStatement.setString(17, data.getAzimuth2());
        preparedStatement.setString(18, data.getSlantRange2());
        preparedStatement.setString(19, data.getIdentifier3());
        preparedStatement.setString(20, data.getSensor3());
        preparedStatement.setString(21, data.getPitch3());
        preparedStatement.setString(22, data.getAzimuth3());
        preparedStatement.setString(23, data.getSlantRange3());
        preparedStatement.setString(24, data.getIdentifier4());
        preparedStatement.setString(25, data.getSensor4());
        preparedStatement.setString(26, data.getPitch4());
        preparedStatement.setString(27, data.getAzimuth4());
        preparedStatement.setString(28, data.getSlantRange4());
        preparedStatement.setString(29, data.getIdentifier5());
        preparedStatement.setString(30, data.getSensor5());
        preparedStatement.setString(31, data.getPitch5());
        preparedStatement.setString(32, data.getAzimuth5());
        preparedStatement.setString(33, data.getSlantRange5());
        preparedStatement.setString(34, data.getIdentifier6());
        preparedStatement.setString(35, data.getSensor6());
        preparedStatement.setString(36, data.getPitch6());
        preparedStatement.setString(37, data.getAzimuth6());
        preparedStatement.setString(38, data.getSlantRange6());
        preparedStatement.setString(39, data.getIdentifier7());
        preparedStatement.setString(40, data.getSensor7());
        preparedStatement.setString(41, data.getPitch7());
        preparedStatement.setString(42, data.getAzimuth7());
        preparedStatement.setString(43, data.getSlantRange7());
        preparedStatement.setString(44, data.getIdentifier8());
        preparedStatement.setString(45, data.getSensor8());
        preparedStatement.setString(46, data.getPitch8());
        preparedStatement.setString(47, data.getAzimuth8());
        preparedStatement.setString(48, data.getSlantRange8());
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
                Long.parseLong(parameterTool.getRequired(IMPORT_ID.getKeyForParamsMap())),
                parameterTool.getRequired(SIMULATION_BATCH_NUMBER.getKeyForParamsMap()));
        TgtFileReceiver receiver = new TgtFileReceiver();
        receiver.setConfig(config);
        receiver.run();
    }
}
