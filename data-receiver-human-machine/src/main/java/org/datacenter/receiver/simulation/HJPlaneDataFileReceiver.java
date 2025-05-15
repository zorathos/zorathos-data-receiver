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
import org.datacenter.model.simulation.HJPlaneData;
import org.datacenter.receiver.simulation.base.SimulationReceiver;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;

import static org.datacenter.config.keys.HumanMachineReceiverConfigKey.IMPORT_ID;
import static org.datacenter.config.keys.HumanMachineReceiverConfigKey.SIMULATION_BATCH_NUMBER;
import static org.datacenter.config.keys.HumanMachineReceiverConfigKey.SIMULATION_URL;


/**
 * @author : [宁]
 * @description : 仿真数据HJPlaneData的接收器
 */
@Slf4j
@Data
@EqualsAndHashCode(callSuper = true)
public class HJPlaneDataFileReceiver extends SimulationReceiver<HJPlaneData> {

    @Override
    public void prepare() {
        table = TiDBTable.HJ_PLANE_DATA;
        modelClass = HJPlaneData.class;
        super.prepare();
    }

    @Override
    protected SerializableFunction<CsvMapper, CsvSchema> getSchemaGenerator() {
        return mapper -> CsvSchema.builder()
                .addColumn("localBatchNumber")              // 批次号
                .addColumn("deviceNumber")             // 设备号
                .addColumn("flightControlNumber")      // 航管号
                .addColumn("localTime")                // 本地时间
                .addColumn("messageTime")              // 消息时间
                .addColumn("messageSequenceNumber")    // 消息序列号
                .addColumn("longitude")                // 经度
                .addColumn("latitude")                 // 纬度
                .addColumn("altitude")                 // 高度
                .addColumn("groundSpeed")              // 地速
                .addColumn("verticalSpeed")            // 垂直速度
                .addColumn("heading")                  // 航向
                .setUseHeader(true)
                .setColumnSeparator(',')
                .setLineSeparator("\n")
                .build();
    }

    @Override
    protected String getInsertQuery() {
        return """
                INSERT INTO `hj_plane_data` (
                    import_id,batch_number, local_batch_number, device_number, flight_control_number, local_time, message_time, message_sequence_number, longitude, latitude, altitude, 
                    ground_speed, vertical_speed, heading
                ) VALUES (
                    ?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                    ?, ?, ?
                );
                """;
    }

    @Override
    protected void bindPreparedStatement(PreparedStatement preparedStatement, HJPlaneData data, String batchNumber, long importId) throws SQLException {
        preparedStatement.setLong(1, importId);        preparedStatement.setString(2, batchNumber);
        preparedStatement.setString(3, data.getLocalBatchNumber());
        preparedStatement.setString(4, data.getDeviceNumber());
        preparedStatement.setString(5, data.getFlightControlNumber());
        preparedStatement.setTime(6, data.getLocalTime() != null ? Time.valueOf(data.getLocalTime()) : null);
        preparedStatement.setTime(7, data.getMessageTime() != null ? Time.valueOf(data.getMessageTime()) : null);
        if (data.getMessageSequenceNumber() != null) {
            preparedStatement.setLong(8, data.getMessageSequenceNumber());
        } else {
            preparedStatement.setNull(8, java.sql.Types.BIGINT);
        }
        preparedStatement.setString(9, data.getLongitude());
        preparedStatement.setString(10, data.getLatitude());
        preparedStatement.setString(11, data.getAltitude());
        preparedStatement.setString(12, data.getGroundSpeed());
        preparedStatement.setString(13, data.getVerticalSpeed());
        preparedStatement.setString(14, data.getHeading());
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
        HJPlaneDataFileReceiver receiver = new HJPlaneDataFileReceiver();
        receiver.setConfig(config);
        receiver.run();
    }
}
