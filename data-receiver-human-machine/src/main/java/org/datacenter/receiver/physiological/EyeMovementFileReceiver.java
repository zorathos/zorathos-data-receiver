package org.datacenter.receiver.physiological;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.jdbc.sink.JdbcSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.csv.CsvReaderFormat;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.datacenter.config.physiological.EyeMovementFileReceiverConfig;
import org.datacenter.exception.ZorathosException;
import org.datacenter.model.base.TiDBDatabase;
import org.datacenter.model.physiological.EyeMovement;
import org.datacenter.receiver.BaseReceiver;
import org.datacenter.receiver.util.DataReceiverUtil;
import org.datacenter.receiver.util.JdbcSinkUtil;

import java.sql.Time;

/**
 * @author : ningaocheng
 * @description :  眼动数据文件接收器
 */

@Slf4j
@Data
@EqualsAndHashCode(callSuper = true)
public class EyeMovementFileReceiver extends BaseReceiver {
    private EyeMovementFileReceiverConfig config;

    @Override
    public void prepare() {
        super.prepare();
    }

    @Override
    public void start() {
        StreamExecutionEnvironment env = DataReceiverUtil.prepareStreamEnv();
        CsvReaderFormat<EyeMovement> csvFormat = CsvReaderFormat.forPojo(EyeMovement.class);
        FileSource<EyeMovement> fileSource = FileSource.forRecordStreamFormat(csvFormat, new Path(config.getUrl())).build();

        Sink<EyeMovement> sink = JdbcSink.<EyeMovement>builder()
                .withQueryStatement("""
                        INSERT INTO eye_movement (
                            record_name, user_name, valid_ratio, time_of_day, video_time, recording_time_stamp, gaze_velocity, serial_send, serial_receive,
                            udp_send, udp_receive, event_label, annotation, validity_left, validity_right, pupil_position_left_x, pupil_position_left_y,
                            pupil_position_right_x, pupil_position_right_y, pupil_diameter_left_px, pupil_diameter_left_mm, pupil_diameter_right_px,
                            pupil_diameter_right_mm, openness_left, openness_right, eyelid_distance_left_px, eyelid_distance_left_mm, eyelid_distance_right_px,
                            eyelid_distance_right_mm, ipd, gaze_point_index, gaze_point_x, gaze_point_y, x_offset, y_offset, gaze_point_left_x, gaze_point_left_y,
                            gaze_point_right_x, gaze_point_right_y, gaze_origin_left_x, gaze_origin_left_y, gaze_origin_left_z, gaze_origin_right_x,
                            gaze_origin_right_y, gaze_origin_right_z, gaze_direction_left_x, gaze_direction_left_y, gaze_direction_left_z,
                            gaze_direction_right_x, gaze_direction_right_y, gaze_direction_right_z, fixation_index, fixation_duration, fixation_point_x,
                            fixation_point_y, saccade_index, saccade_duration, saccade_amplitude, saccade_velocity_average, saccade_velocity_peak,
                            invalid_index, invalid_duration, blink_index, blink_duration, blink_eye, quat_data, gyro, accel, mag
                        ) VALUES (
                            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                        ) ON DUPLICATE KEY UPDATE
                            record_name = VALUES(record_name), user_name = VALUES(user_name), valid_ratio = VALUES(valid_ratio), time_of_day = VALUES(time_of_day),
                            video_time = VALUES(video_time), recording_time_stamp = VALUES(recording_time_stamp), gaze_velocity = VALUES(gaze_velocity),
                            serial_send = VALUES(serial_send), serial_receive = VALUES(serial_receive), udp_send = VALUES(udp_send), udp_receive = VALUES(udp_receive),
                            event_label = VALUES(event_label), annotation = VALUES(annotation), validity_left = VALUES(validity_left), validity_right = VALUES(validity_right),
                            pupil_position_left_x = VALUES(pupil_position_left_x), pupil_position_left_y = VALUES(pupil_position_left_y), pupil_position_right_x = VALUES(pupil_position_right_x),
                            pupil_position_right_y = VALUES(pupil_position_right_y), pupil_diameter_left_px = VALUES(pupil_diameter_left_px), pupil_diameter_left_mm = VALUES(pupil_diameter_left_mm),
                            pupil_diameter_right_px = VALUES(pupil_diameter_right_px), pupil_diameter_right_mm = VALUES(pupil_diameter_right_mm), openness_left = VALUES(openness_left),
                            openness_right = VALUES(openness_right), eyelid_distance_left_px = VALUES(eyelid_distance_left_px), eyelid_distance_left_mm = VALUES(eyelid_distance_left_mm),
                            eyelid_distance_right_px = VALUES(eyelid_distance_right_px), eyelid_distance_right_mm = VALUES(eyelid_distance_right_mm), ipd = VALUES(ipd),
                            gaze_point_index = VALUES(gaze_point_index), gaze_point_x = VALUES(gaze_point_x), gaze_point_y = VALUES(gaze_point_y), x_offset = VALUES(x_offset),
                            y_offset = VALUES(y_offset), gaze_point_left_x = VALUES(gaze_point_left_x), gaze_point_left_y = VALUES(gaze_point_left_y),
                            gaze_point_right_x = VALUES(gaze_point_right_x), gaze_point_right_y = VALUES(gaze_point_right_y), gaze_origin_left_x = VALUES(gaze_origin_left_x),
                            gaze_origin_left_y = VALUES(gaze_origin_left_y), gaze_origin_left_z = VALUES(gaze_origin_left_z), gaze_origin_right_x = VALUES(gaze_origin_right_x),
                            gaze_origin_right_y = VALUES(gaze_origin_right_y), gaze_origin_right_z = VALUES(gaze_origin_right_z), gaze_direction_left_x = VALUES(gaze_direction_left_x),
                            gaze_direction_left_y = VALUES(gaze_direction_left_y), gaze_direction_left_z = VALUES(gaze_direction_left_z), gaze_direction_right_x = VALUES(gaze_direction_right_x),
                            gaze_direction_right_y = VALUES(gaze_direction_right_y), gaze_direction_right_z = VALUES(gaze_direction_right_z), fixation_index = VALUES(fixation_index),
                            fixation_duration = VALUES(fixation_duration), fixation_point_x = VALUES(fixation_point_x), fixation_point_y = VALUES(fixation_point_y),
                            saccade_index = VALUES(saccade_index), saccade_duration = VALUES(saccade_duration), saccade_amplitude = VALUES(saccade_amplitude),
                            saccade_velocity_average = VALUES(saccade_velocity_average), saccade_velocity_peak = VALUES(saccade_velocity_peak), invalid_index = VALUES(invalid_index),
                            invalid_duration = VALUES(invalid_duration), blink_index = VALUES(blink_index), blink_duration = VALUES(blink_duration), blink_eye = VALUES(blink_eye),
                            quat_data = VALUES(quat_data), gyro = VALUES(gyro), accel = VALUES(accel), mag = VALUES(mag);
                        """, (JdbcStatementBuilder<EyeMovement>) (preparedStatement, eyeMovement) -> {
                    preparedStatement.setString(1, eyeMovement.getRecordName());
                    preparedStatement.setString(2, eyeMovement.getUserName());
                    preparedStatement.setDouble(3, eyeMovement.getValidRatio());
                    preparedStatement.setTime(4, Time.valueOf(eyeMovement.getTimeOfDay()));
                    preparedStatement.setTime(5, Time.valueOf(eyeMovement.getVideoTime()));
                    preparedStatement.setLong(6, eyeMovement.getRecordingTimeStamp());
                    preparedStatement.setDouble(7, eyeMovement.getGazeVelocity());
                    preparedStatement.setLong(8, eyeMovement.getSerialSend());
                    preparedStatement.setLong(9, eyeMovement.getSerialReceive());
                    preparedStatement.setLong(10, eyeMovement.getUdpSend());
                    preparedStatement.setLong(11, eyeMovement.getUdpReceive());
                    preparedStatement.setString(12, eyeMovement.getEventLabel());
                    preparedStatement.setString(13, eyeMovement.getAnnotation());
                    preparedStatement.setDouble(14, eyeMovement.getValidityLeft());
                    preparedStatement.setDouble(15, eyeMovement.getValidityRight());
                    preparedStatement.setDouble(16, eyeMovement.getPupilPositionLeftX());
                    preparedStatement.setDouble(17, eyeMovement.getPupilPositionLeftY());
                    preparedStatement.setDouble(18, eyeMovement.getPupilPositionRightX());
                    preparedStatement.setDouble(19, eyeMovement.getPupilPositionRightY());
                    preparedStatement.setDouble(20, eyeMovement.getPupilDiameterLeftPx());
                    preparedStatement.setDouble(21, eyeMovement.getPupilDiameterLeftMm());
                    preparedStatement.setDouble(22, eyeMovement.getPupilDiameterRightPx());
                    preparedStatement.setDouble(23, eyeMovement.getPupilDiameterRightMm());
                    preparedStatement.setDouble(24, eyeMovement.getOpennessLeft());
                    preparedStatement.setDouble(25, eyeMovement.getOpennessRight());
                    preparedStatement.setDouble(26, eyeMovement.getEyelidDistanceLeftPx());
                    preparedStatement.setDouble(27, eyeMovement.getEyelidDistanceLeftMm());
                    preparedStatement.setDouble(28, eyeMovement.getEyelidDistanceRightPx());
                    preparedStatement.setDouble(29, eyeMovement.getEyelidDistanceRightMm());
                    preparedStatement.setDouble(30, eyeMovement.getIpd());
                    preparedStatement.setLong(31, eyeMovement.getGazePointIndex());
                    preparedStatement.setDouble(32, eyeMovement.getGazePointX());
                    preparedStatement.setDouble(33, eyeMovement.getGazePointY());
                    preparedStatement.setDouble(34, eyeMovement.getXOffset());
                    preparedStatement.setDouble(35, eyeMovement.getYOffset());
                    preparedStatement.setDouble(36, eyeMovement.getGazePointLeftX());
                    preparedStatement.setDouble(37, eyeMovement.getGazePointLeftY());
                    preparedStatement.setDouble(38, eyeMovement.getGazePointRightX());
                    preparedStatement.setDouble(39, eyeMovement.getGazePointRightY());
                    preparedStatement.setDouble(40, eyeMovement.getGazeOriginLeftX());
                    preparedStatement.setDouble(41, eyeMovement.getGazeOriginLeftY());
                    preparedStatement.setDouble(42, eyeMovement.getGazeOriginLeftZ());
                    preparedStatement.setDouble(43, eyeMovement.getGazeOriginRightX());
                    preparedStatement.setDouble(44, eyeMovement.getGazeOriginRightY());
                    preparedStatement.setDouble(45, eyeMovement.getGazeOriginRightZ());
                    preparedStatement.setDouble(46, eyeMovement.getGazeDirectionLeftX());
                    preparedStatement.setDouble(47, eyeMovement.getGazeDirectionLeftY());
                    preparedStatement.setDouble(48, eyeMovement.getGazeDirectionLeftZ());
                    preparedStatement.setDouble(49, eyeMovement.getGazeDirectionRightX());
                    preparedStatement.setDouble(50, eyeMovement.getGazeDirectionRightY());
                    preparedStatement.setDouble(51, eyeMovement.getGazeDirectionRightZ());
                    preparedStatement.setLong(52, eyeMovement.getFixationIndex());
                    preparedStatement.setDouble(53, eyeMovement.getFixationDuration());
                    preparedStatement.setDouble(54, eyeMovement.getFixationPointX());
                    preparedStatement.setDouble(55, eyeMovement.getFixationPointY());
                    preparedStatement.setLong(56, eyeMovement.getSaccadeIndex());
                    preparedStatement.setDouble(57, eyeMovement.getSaccadeDuration());
                    preparedStatement.setDouble(58, eyeMovement.getSaccadeAmplitude());
                    preparedStatement.setDouble(59, eyeMovement.getSaccadeVelocityAverage());
                    preparedStatement.setDouble(60, eyeMovement.getSaccadeVelocityPeak());
                    preparedStatement.setLong(61, eyeMovement.getInvalidIndex());
                    preparedStatement.setDouble(62, eyeMovement.getInvalidDuration());
                    preparedStatement.setLong(63, eyeMovement.getBlinkIndex());
                    preparedStatement.setDouble(64, eyeMovement.getBlinkDuration());
                    preparedStatement.setString(65, eyeMovement.getBlinkEye());
                    preparedStatement.setString(66, eyeMovement.getQuatData());
                    preparedStatement.setString(67, eyeMovement.getGyro());
                    preparedStatement.setString(68, eyeMovement.getAccel());
                    preparedStatement.setString(69, eyeMovement.getMag());
                })
                .withExecutionOptions(JdbcSinkUtil.getTiDBJdbcExecutionOptions())
                .buildAtLeastOnce(JdbcSinkUtil.getTiDBJdbcConnectionOptions(TiDBDatabase.PHYSIOLOGICAL));

        env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "FileSource")
                .sinkTo(sink)
                .name("EyeMovement File Sink");

        try {
            env.execute();
        } catch (Exception e) {
            throw new ZorathosException(e, "Encounter error when sinking EyeMovement to tidb.");
        }
    }

    /**
     * 输入参数形式 --url "s3://bucket-name/path/to/file.csv"
     * @param args 入参
     */
    public static void main(String[] args) {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        EyeMovementFileReceiverConfig config = EyeMovementFileReceiverConfig.builder()
                .url(parameterTool.getRequired("url"))
                .build();

        EyeMovementFileReceiver receiver = new EyeMovementFileReceiver();
        receiver.setConfig(config);
        receiver.run();
    }
}
