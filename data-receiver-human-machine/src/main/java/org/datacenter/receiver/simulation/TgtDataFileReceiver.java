package org.datacenter.receiver.simulation;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.csv.CsvReaderFormat;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.datacenter.config.simulation.TgtDataReceiverConfig;
import org.datacenter.exception.ZorathosException;
import org.datacenter.model.simulation.TgtData;
import org.datacenter.receiver.BaseReceiver;
import org.datacenter.receiver.util.DataReceiverUtil;
import org.datacenter.receiver.util.JdbcSinkUtil;

import java.sql.Timestamp;

/**
 * @author : [wangminan]
 * @description : TgtData文件接收器
 */
@Slf4j
@SuppressWarnings("deprecation")
public class TgtDataFileReceiver extends BaseReceiver {

    private static TgtDataReceiverConfig config;

    @Override
    public void prepare() {
    }

    @Override
    public void start() {
        StreamExecutionEnvironment env = DataReceiverUtil.prepareStreamEnv();

        // 使用官方的CSV处理器
        CsvReaderFormat<TgtData> csvFormat = CsvReaderFormat.forPojo(TgtData.class);
        FileSource<TgtData> tgtDataSource = FileSource.forRecordStreamFormat(csvFormat, new Path(config.getUrl())).build();

        SinkFunction<TgtData> sinkFunction = JdbcSink.sink("""
                INSERT INTO tgt_data (id, timestamp, target_count, target1_status, target1_sensor, target1_azimuth, target1_elevation, target1_slant_range, reserved)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON DUPLICATE KEY UPDATE
                    timestamp = VALUES(timestamp),
                    target_count = VALUES(target_count),
                    target1_status = VALUES(target1_status),
                    target1_sensor = VALUES(target1_sensor),
                    target1_azimuth = VALUES(target1_azimuth),
                    target1_elevation = VALUES(target1_elevation),
                    target1_slant_range = VALUES(target1_slant_range),
                    reserved = VALUES(reserved);
                """, (preparedStatement, tgtData) -> {
            preparedStatement.setLong(1, tgtData.getId());
            preparedStatement.setTimestamp(2, Timestamp.valueOf(tgtData.getTimestamp()));
            preparedStatement.setInt(3, tgtData.getTargetCount());
            preparedStatement.setByte(4, tgtData.getTarget1Status());
            preparedStatement.setByte(5, tgtData.getTarget1Sensor());
            preparedStatement.setInt(6, tgtData.getTarget1Azimuth());
            preparedStatement.setInt(7, tgtData.getTarget1Elevation());
            preparedStatement.setInt(8, tgtData.getTarget1SlantRange());
            preparedStatement.setInt(9, tgtData.getReserved());
        }, JdbcSinkUtil.getTiDBJdbcExecutionOptions(), JdbcSinkUtil.getTiDBJdbcConnectionOptions());

        env.fromSource(tgtDataSource, WatermarkStrategy.noWatermarks(), "TgtDataSource")
                .addSink(sinkFunction)
                .name("TgtDataSink");
        try {
            env.execute("TgtDataSink");
        } catch (Exception e) {
            throw new ZorathosException(e, "Failed to execute TgtDataFileReceiver");
        }
    }

    public static void main(String[] args) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            config = mapper.readValue(args[0], TgtDataReceiverConfig.class);
        } catch (JsonProcessingException e) {
            throw new ZorathosException(e, "Failed to parse tspi config from json string");
        }
        TgtDataFileReceiver receiver = new TgtDataFileReceiver();
        receiver.run();
    }
}
