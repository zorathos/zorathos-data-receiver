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
import org.datacenter.config.simulation.RtsnReceiverConfig;
import org.datacenter.exception.ZorathosException;
import org.datacenter.model.simulation.Rtsn;
import org.datacenter.receiver.BaseReceiver;
import org.datacenter.receiver.util.DataReceiverUtil;
import org.datacenter.receiver.util.JdbcSinkUtil;

import java.sql.Timestamp;

/**
 * @author : [wangminan]
 * @description : Rtsn文件接收器
 */
@Slf4j
@SuppressWarnings("deprecation")
public class RtsnFileReceiver extends BaseReceiver {

    private static RtsnReceiverConfig config;

    @Override
    public void prepare() {
    }

    @Override
    public void start() {
        StreamExecutionEnvironment env = DataReceiverUtil.prepareStreamEnv();
        // 使用官方的CSV处理器
        CsvReaderFormat<Rtsn> csvFormat = CsvReaderFormat.forPojo(Rtsn.class);
        FileSource<Rtsn> rtsnSource = FileSource.forRecordStreamFormat(csvFormat, new Path(config.getUrl())).build();

        SinkFunction<Rtsn> sinkFunction = JdbcSink.sink("""
                INSERT INTO rtsn (id, timestamp, target_id, target_type, weapon_type, pylon_id, weapon_id)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON DUPLICATE KEY UPDATE
                timestamp = VALUES(timestamp),
                target_id = VALUES(target_id),
                target_type = VALUES(target_type),
                weapon_type = VALUES(weapon_type),
                pylon_id = VALUES(pylon_id),
                weapon_id = VALUES(weapon_id);
                """, (preparedStatement, rtsn) -> {
            preparedStatement.setLong(1, rtsn.getId());
            preparedStatement.setTimestamp(2, Timestamp.valueOf(rtsn.getTimestamp()));
            preparedStatement.setLong(3, rtsn.getTargetId());
            preparedStatement.setLong(4, rtsn.getTargetType());
            preparedStatement.setLong(5, rtsn.getWeaponType());
            preparedStatement.setLong(6, rtsn.getPylonId());
            preparedStatement.setLong(7, rtsn.getWeaponId());
        }, JdbcSinkUtil.getTiDBJdbcExecutionOptions(), JdbcSinkUtil.getTiDBJdbcConnectionOptions());

        env.fromSource(rtsnSource, WatermarkStrategy.noWatermarks(), "RtsnSource")
                .addSink(sinkFunction).name("RtsnSink");

        try {
            env.execute("RtsnSink");
        } catch (Exception e) {
            throw new ZorathosException(e, "Failed to execute flink job");
        }
    }

    public static void main(String[] args) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            config = mapper.readValue(args[0], RtsnReceiverConfig.class);
        } catch (JsonProcessingException e) {
            throw new ZorathosException(e, "Failed to parse tspi config from json string");
        }
        RtsnFileReceiver rtsnFileReceiver = new RtsnFileReceiver();
        rtsnFileReceiver.run();
    }
}
