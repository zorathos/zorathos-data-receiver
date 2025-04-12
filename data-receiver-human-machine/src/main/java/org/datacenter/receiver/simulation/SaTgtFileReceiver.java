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
import org.datacenter.model.simulation.SaTgt;
import org.datacenter.receiver.simulation.base.SimulationReceiver;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;


/**
 * @author : [宁]
 * @description : 仿真数据SaTgt的接收器
 */
@Slf4j
@Data
@EqualsAndHashCode(callSuper = true)
public class SaTgtFileReceiver extends SimulationReceiver<SaTgt> {

    @Override
    public void prepare() {
        table = TiDBTable.SA_TGT;
        modelClass = SaTgt.class;
        super.prepare();
    }

    @Override
    protected SerializableFunction<CsvMapper, CsvSchema> getSchemaGenerator() {
        return mapper -> CsvSchema.builder()
                .addColumn("groundDefenseId")         // 地防ID
                .addColumn("messageTime")             // 消息时间
                .addColumn("satelliteGuidanceTime")   // 卫导时间
                .addColumn("localTime")             // 本地时间
                .addColumn("messageSequenceNumber") // 消息序列号
                .addColumn("equipmentType")           // 装备类型
                .addColumn("equipmentNumber")         // 装备编号
                .addColumn("targetAzimuth")           // 目标方位角
                .addColumn("targetElevation")         // 目标俯仰角
                .addColumn("targetSlantRange")        // 目标斜距
                .addColumn("channelNumber")           // 通道号
                .addColumn("targetBatchNumber")       // 目标批号
                .setUseHeader(true)
                .setColumnSeparator(',')
                .setLineSeparator("\n")
                .build();
    }

    @Override
    protected String getInsertQuery() {
        return """
                INSERT INTO `sa_tgt` (
                    sortie_number, ground_defense_id, message_time, satellite_guidance_time, local_time, message_sequence_number, equipment_type, equipment_number, target_azimuth, 
                    target_elevation, target_slant_range, channel_number, target_batch_number
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                    ?, ?, ?, ?
                );
                """;
    }

    @Override
    protected void bindPreparedStatement(PreparedStatement preparedStatement, SaTgt data, String sortieNumber) throws SQLException {
// 注意 sortieNumber 是从配置里面来的 csv里面没有
        preparedStatement.setString(1, sortieNumber);
        preparedStatement.setString(2, data.getGroundDefenseId());
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
        preparedStatement.setString(7, data.getEquipmentType());
        preparedStatement.setString(8, data.getEquipmentNumber());
        preparedStatement.setString(9, data.getTargetAzimuth());
        preparedStatement.setString(10, data.getTargetElevation());
        preparedStatement.setString(11, data.getTargetSlantRange());
        preparedStatement.setString(12, data.getChannelNumber());
        preparedStatement.setString(13, data.getTargetBatchNumber());
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
        SaTgtFileReceiver receiver = new SaTgtFileReceiver();
        receiver.setConfig(config);
        receiver.run();
    }
}
