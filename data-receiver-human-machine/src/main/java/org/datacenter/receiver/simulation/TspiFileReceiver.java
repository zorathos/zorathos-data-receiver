package org.datacenter.receiver.simulation;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.csv.CsvReaderFormat;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.datacenter.config.simulation.TspiReceiverConfig;
import org.datacenter.exception.ZorathosException;
import org.datacenter.model.simulation.Tspi;
import org.datacenter.receiver.BaseReceiver;
import org.datacenter.receiver.util.DataReceiverUtil;
import org.datacenter.receiver.util.JdbcSinkUtil;

import java.sql.Timestamp;

/**
 * @author : [wangminan]
 * @description :  Tspi文件接收器
 */
@Slf4j
@SuppressWarnings("deprecation")
public class TspiFileReceiver extends BaseReceiver {
    private static TspiReceiverConfig config;

    @Override
    public void prepare() {
    }

    @Override
    public void start() {
        StreamExecutionEnvironment env = DataReceiverUtil.prepareStreamEnv();

        // 使用官方的CSV处理器
        CsvReaderFormat<Tspi> csvFormat = CsvReaderFormat.forPojo(Tspi.class);
        FileSource<Tspi> fileSource = FileSource.forRecordStreamFormat(csvFormat, new Path(config.getUrl())).build();

        SinkFunction<Tspi> sinkFunction = JdbcSink.sink("""
                INSERT INTO tspi (id, timestamp, inertial_pressure_altitude, true_heading, longitude, latitude, pitch_angle, roll_angle, aircraft_training_status, north_velocity, east_velocity, aircraft_attribute, engine_afterburner_status, infrared_decoy_release, upward_velocity, platform_type, sat_inertial_altitude_diff, reserved)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON DUPLICATE KEY UPDATE
                    timestamp = VALUES(timestamp),
                    inertial_pressure_altitude = VALUES(inertial_pressure_altitude),
                    true_heading = VALUES(true_heading),
                    longitude = VALUES(longitude),
                    latitude = VALUES(latitude),
                    pitch_angle = VALUES(pitch_angle),
                    roll_angle = VALUES(roll_angle),
                    aircraft_training_status = VALUES(aircraft_training_status),
                    north_velocity = VALUES(north_velocity),
                    east_velocity = VALUES(east_velocity),
                    aircraft_attribute = VALUES(aircraft_attribute),
                    engine_afterburner_status = VALUES(engine_afterburner_status),
                    infrared_decoy_release = VALUES(infrared_decoy_release),
                    upward_velocity = VALUES(upward_velocity),
                    platform_type = VALUES(platform_type),
                    sat_inertial_altitude_diff = VALUES(sat_inertial_altitude_diff),
                    reserved = VALUES(reserved);
                """, (JdbcStatementBuilder<Tspi>) (preparedStatement, tspi) -> {
            preparedStatement.setLong(1, tspi.getId());
            preparedStatement.setTimestamp(2, Timestamp.valueOf(tspi.getTimestamp()));
            preparedStatement.setDouble(3, tspi.getInertialPressureAltitude());
            preparedStatement.setDouble(4, tspi.getTrueHeading());
            preparedStatement.setDouble(5, tspi.getLongitude());
            preparedStatement.setDouble(6, tspi.getLatitude());
            preparedStatement.setDouble(7, tspi.getPitchAngle());
            preparedStatement.setDouble(8, tspi.getRollAngle());
            preparedStatement.setInt(9, tspi.getAircraftTrainingStatus());
            preparedStatement.setDouble(10, tspi.getNorthVelocity());
            preparedStatement.setDouble(11, tspi.getEastVelocity());
            preparedStatement.setInt(12, tspi.getAircraftAttribute());
            preparedStatement.setInt(13, tspi.getEngineAfterburnerStatus());
            preparedStatement.setInt(14, tspi.getInfraredDecoyRelease());
            preparedStatement.setDouble(15, tspi.getUpwardVelocity());
            preparedStatement.setInt(16, tspi.getPlatformType());
            preparedStatement.setDouble(17, tspi.getSatInertialAltitudeDiff());
            preparedStatement.setInt(18, tspi.getReserved());
        }, JdbcSinkUtil.getTiDBJdbcExecutionOptions(), JdbcSinkUtil.getTiDBJdbcConnectionOptions());

        env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "FileSource")
                .addSink(sinkFunction)
                .name("Tspi File Sink");

        try {
            env.execute();
        } catch (Exception e) {
            throw new ZorathosException(e, "Encounter error when sinking flight plan data to tidb.");
        }
    }

    public static void main(String[] args) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            config = mapper.readValue(args[0], TspiReceiverConfig.class);
        } catch (JsonProcessingException e) {
            throw new ZorathosException(e, "Failed to parse tspi config from json string");
        }
        TspiFileReceiver receiver = new TspiFileReceiver();
        receiver.run();
    }
}
