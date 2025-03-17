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
import org.datacenter.config.simulation.RdrStateReceiverConfig;
import org.datacenter.exception.ZorathosException;
import org.datacenter.model.simulation.RdrState;
import org.datacenter.receiver.BaseReceiver;
import org.datacenter.receiver.util.DataReceiverUtil;
import org.datacenter.receiver.util.JdbcSinkUtil;

import java.sql.Timestamp;

/**
 * @author : [wangminan]
 * @description : Rdr文件接收器
 */
@Slf4j
@SuppressWarnings("deprecation")
public class RdrStateFileReceiver extends BaseReceiver {

    private static RdrStateReceiverConfig config;

    @Override
    public void prepare() {

    }

    @Override
    public void start() {
        StreamExecutionEnvironment env = DataReceiverUtil.prepareStreamEnv();

        // 使用官方的CSV处理器
        CsvReaderFormat<RdrState> csvFormat = CsvReaderFormat.forPojo(RdrState.class);
        FileSource<RdrState> rdrStateSource = FileSource.forRecordStreamFormat(csvFormat, new Path(config.getUrl())).build();

        SinkFunction<RdrState> sinkFunction = JdbcSink.sink("""
                        INSERT INTO rdr_state (id, timestamp, radar_mode, power_status, radiation_status, reserved)
                        VALUES (?, ?, ?, ?, ?, ?)
                        ON DUPLICATE KEY UPDATE
                        timestamp = VALUES(timestamp),
                        radar_mode = VALUES(radar_mode),
                        power_status = VALUES(power_status),
                        radiation_status = VALUES(radiation_status),
                        reserved = VALUES(reserved);
                        """, (preparedStatement, rdrState) -> {
                    preparedStatement.setLong(1, rdrState.getId());
                    preparedStatement.setTimestamp(2, Timestamp.valueOf(rdrState.getTimestamp()));
                    preparedStatement.setShort(3, rdrState.getRadarMode());
                    preparedStatement.setShort(4, rdrState.getPowerStatus());
                    preparedStatement.setShort(5, rdrState.getRadiationStatus());
                    preparedStatement.setInt(6, rdrState.getReserved());
                },
                JdbcSinkUtil.getTiDBJdbcExecutionOptions(), JdbcSinkUtil.getTiDBJdbcConnectionOptions());

        env.fromSource(rdrStateSource, WatermarkStrategy.noWatermarks(), "RdrStateSource").addSink(sinkFunction).name("RdrStateSink");
        try {
            env.execute("RdrStateSink");
        } catch (Exception e) {
            throw new ZorathosException(e, "Encounter error when sinking flight plan data to tidb.");
        }
    }

    public static void main(String[] args) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            config = mapper.readValue(args[0], RdrStateReceiverConfig.class);
        } catch (JsonProcessingException e) {
            throw new ZorathosException(e, "Failed to parse tspi config from json string");
        }
        RdrStateFileReceiver rdrStateFileReceiver = new RdrStateFileReceiver();
        rdrStateFileReceiver.run();
    }
}
