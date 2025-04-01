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
import org.datacenter.model.simulation.Ews;
import org.datacenter.receiver.simulation.base.SimulationReceiver;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;


/**
 * @author : [宁]
 * @description : 仿真数据Ews的接收器
 */
@Slf4j
@Data
@EqualsAndHashCode(callSuper = true)
public class EwsFileReceiver extends SimulationReceiver<Ews> {

    @Override
    public void prepare() {
        table = TiDBTable.EWS;
        modelClass = Ews.class;
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
                .addColumn("leftFrontJamming")        // 左前干扰
                .addColumn("rightFrontJamming")       // 右前干扰
                .addColumn("leftRearJamming")         // 左后干扰
                .addColumn("rightRearJamming")        // 右后干扰
                .addColumn("podForwardJamming")       // 吊舱前向干扰
                .addColumn("podRearwardJamming")      // 吊舱后向干扰
                .setUseHeader(true)
                .setColumnSeparator(',')
                .setLineSeparator("\n")
                .build();
    }

    @Override
    protected String getInsertQuery() {
        return """
                INSERT INTO `ews` (
                    sortie_number, aircraft_id, message_time, satellite_guidance_time, local_time, message_sequence_number, left_front_jamming, right_front_jamming, left_rear_jamming, right_rear_jamming, 
                    pod_forward_jamming, pod_rearward_jamming
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                    ?, ?
                );
                """;
    }

    @Override
    protected void bindPreparedStatement(PreparedStatement preparedStatement, Ews data, String sortieNumber) throws SQLException {
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
        preparedStatement.setString(7, data.getLeftFrontJamming());
        preparedStatement.setString(8, data.getRightFrontJamming());
        preparedStatement.setString(9, data.getLeftRearJamming());
        preparedStatement.setString(10, data.getRightRearJamming());
        preparedStatement.setString(11, data.getPodForwardJamming());
        preparedStatement.setString(12, data.getPodRearwardJamming());
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
        EwsFileReceiver receiver = new EwsFileReceiver();
        receiver.setConfig(config);
        receiver.run();
    }
}
