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
import org.datacenter.config.simulation.RtknReceiverConfig;
import org.datacenter.exception.ZorathosException;
import org.datacenter.model.simulation.Rtkn;
import org.datacenter.receiver.BaseReceiver;
import org.datacenter.receiver.util.DataReceiverUtil;
import org.datacenter.receiver.util.JdbcSinkUtil;

import java.sql.Timestamp;

/**
 * @author : [wangminan]
 * @description : Rtkn文件接收器
 */
@Slf4j
@SuppressWarnings("deprecation")
public class RtknFileReceiver extends BaseReceiver {

    private static RtknReceiverConfig config;

    @Override
    public void prepare() {

    }

    @Override
    public void start() {
        StreamExecutionEnvironment env = DataReceiverUtil.prepareStreamEnv();
        // 使用官方的CSV处理器
        CsvReaderFormat<Rtkn> csvFormat = CsvReaderFormat.forPojo(Rtkn.class);
        FileSource<Rtkn> rtknSource = FileSource.forRecordStreamFormat(csvFormat, new Path(config.getUrl())).build();

        SinkFunction<Rtkn> sinkFunction = JdbcSink.sink("""
                INSERT INTO rtkn (id, timestamp, target_type, weapon_id, weapon_type, missed_reason, match_fail_reason, miss_distance, hit_result, reserved)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON DUPLICATE KEY UPDATE
                timestamp = VALUES(timestamp),
                target_type = VALUES(target_type),
                weapon_id = VALUES(weapon_id),
                weapon_type = VALUES(weapon_type),
                missed_reason = VALUES(missed_reason),
                match_fail_reason = VALUES(match_fail_reason),
                miss_distance = VALUES(miss_distance),
                hit_result = VALUES(hit_result),
                reserved = VALUES(reserved);
                """, (preparedStatement, rtkn) -> {
            preparedStatement.setLong(1, rtkn.getId());
            preparedStatement.setTimestamp(2, Timestamp.valueOf(rtkn.getTimestamp()));
            preparedStatement.setShort(3, rtkn.getTargetType());
            preparedStatement.setShort(4, rtkn.getWeaponId());
            preparedStatement.setShort(5, rtkn.getWeaponType());
            preparedStatement.setShort(6, rtkn.getMissedReason());
            preparedStatement.setShort(7, rtkn.getMatchFailReason());
            preparedStatement.setDouble(8, rtkn.getMissDistance());
            preparedStatement.setShort(9, rtkn.getHitResult());
            preparedStatement.setInt(10, rtkn.getReserved());
        }, JdbcSinkUtil.getTiDBJdbcExecutionOptions(), JdbcSinkUtil.getTiDBJdbcConnectionOptions());

        env.fromSource(rtknSource, WatermarkStrategy.noWatermarks(), "RtknSource").addSink(sinkFunction).name("RtknSink");

        try {
            env.execute();
        } catch (Exception e) {
            log.error("RtknFileReceiver start error", e);
        }
    }

    public static void main(String[] args) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            config = mapper.readValue(args[0], RtknReceiverConfig.class);
        } catch (JsonProcessingException e) {
            throw new ZorathosException(e, "Failed to parse tspi config from json string");
        }
        RtknFileReceiver rtknFileReceiver = new RtknFileReceiver();
        rtknFileReceiver.run();
    }
}
