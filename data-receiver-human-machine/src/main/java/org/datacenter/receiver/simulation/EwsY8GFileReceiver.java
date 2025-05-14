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
import org.datacenter.model.simulation.EwsY8G;
import org.datacenter.receiver.simulation.base.SimulationReceiver;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;

import static org.datacenter.config.keys.HumanMachineReceiverConfigKey.IMPORT_ID;
import static org.datacenter.config.keys.HumanMachineReceiverConfigKey.SIMULATION_BATCH_NUMBER;
import static org.datacenter.config.keys.HumanMachineReceiverConfigKey.SIMULATION_URL;

/**
 * @author : [宁]
 * @description : 仿真数据EwsY8G的接收器
 */
@Slf4j
@Data
@EqualsAndHashCode(callSuper = true)
public class EwsY8GFileReceiver extends SimulationReceiver<EwsY8G> {

    @Override
    public void prepare() {
        table = TiDBTable.EWS_Y8G;
        modelClass = EwsY8G.class;
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
                .addColumn("xBandJammingElevation")   // X波段干扰俯仰
                .addColumn("cBandJammingElevation")   // C波段干扰俯仰
                .addColumn("cBandJammingAzimuth")     // C波段干扰方位
                .addColumn("sBandJammingElevation")   // S波段干扰俯仰
                .addColumn("sBandJammingAzimuth")     // S波段干扰方位
                .addColumn("lBandJammingElevation")   // L波段干扰俯仰
                .addColumn("lBandJammingAzimuth")     // L波段干扰方位
                .addColumn("uBandJammingAzimuth")     // U波段干扰方位
                .addColumn("jammingStatus")           // 干扰状态
                .addColumn("xBandJammingAzimuthAngle")// X波段干扰方位角
                .setUseHeader(true)
                .setColumnSeparator(',')
                .setLineSeparator("\n")
                .build();
    }

    @Override
    protected String getInsertQuery() {
        return """
                INSERT INTO `ews_y8g` (
                    import_id,batch_number, aircraft_id, message_time, satellite_guidance_time, local_time, message_sequence_number, x_band_jamming_elevation, c_band_jamming_elevation, c_band_jamming_azimuth, s_band_jamming_elevation, 
                    s_band_jamming_azimuth, l_band_jamming_elevation, l_band_jamming_azimuth, u_band_jamming_azimuth, jamming_status, x_band_jamming_azimuth_angle
                ) VALUES (
                    ?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                    ?, ?, ?, ?, ?, ?
                );
                """;
    }

    @Override
    protected void bindPreparedStatement(PreparedStatement preparedStatement, EwsY8G data, String batchNumber, long importId) throws SQLException {
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
        preparedStatement.setString(8, data.getXBandJammingElevation());
        preparedStatement.setString(9, data.getCBandJammingElevation());
        preparedStatement.setString(10, data.getCBandJammingAzimuth());
        preparedStatement.setString(11, data.getSBandJammingElevation());
        preparedStatement.setString(12, data.getSBandJammingAzimuth());
        preparedStatement.setString(13, data.getLBandJammingElevation());
        preparedStatement.setString(14, data.getLBandJammingAzimuth());
        preparedStatement.setString(15, data.getUBandJammingAzimuth());
        preparedStatement.setString(16, data.getJammingStatus());
        preparedStatement.setString(17, data.getXBandJammingAzimuthAngle());
    }

    @Override
    public void start() {
        super.start();
    }

    // 参数输入形式为 --url s3://human-machine/simulation/simulated_data_large.csv --import_id 20250303_五_01_ACT-3_邱陈_J16_07#02 --batch_number 20250303_五_01_ACT-3_邱陈_J16_07#02
    public static void main(String[] args) {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        SimulationReceiverConfig config = new SimulationReceiverConfig(
                parameterTool.getRequired(SIMULATION_URL.getKeyForParamsMap()),
                parameterTool.getRequired((IMPORT_ID.getKeyForParamsMap())),
                parameterTool.getRequired(SIMULATION_BATCH_NUMBER.getKeyForParamsMap()));
        EwsY8GFileReceiver receiver = new EwsY8GFileReceiver();
        receiver.setConfig(config);
        receiver.run();
    }
}
