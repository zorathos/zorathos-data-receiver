package org.datacenter.receiver.physiological.input;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.jdbc.sink.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.datacenter.config.receiver.physiological.PhysiologicalKafkaReceiverConfig;
import org.datacenter.exception.ZorathosException;
import org.datacenter.model.base.TiDBDatabase;
import org.datacenter.model.physiological.input.EyeTracking;
import org.datacenter.receiver.physiological.base.PhysiologicalKafkaReceiver;
import org.datacenter.receiver.util.DataReceiverUtil;
import org.datacenter.receiver.util.JdbcSinkUtil;

import java.util.List;

/**
 * @author : [wangminan]
 * @description : 眼动在线接收器
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Slf4j
public class EyeTrackingKafkaReceiver extends PhysiologicalKafkaReceiver<EyeTracking> {

    public static void main(String[] args) {
        PhysiologicalKafkaReceiverConfig config = parseArgs(args);
        EyeTrackingKafkaReceiver receiver = new EyeTrackingKafkaReceiver();
        receiver.setConfig(config);
        receiver.run();
    }

    @Override
    public void prepare() {
        super.prepare();
        modelClass = EyeTracking.class;
    }

    @Override
    protected Sink<EyeTracking> createSink(Long importId) {
        return JdbcSink.<EyeTracking>builder()
                .withQueryStatement("""
                        INSERT INTO eye_tracking (
                            pilot_id, task_id, device_id, timestamp, import_id
                            pupil_diameter_left_px, pupil_diameter_left_mm, pupil_diameter_right_px, pupil_diameter_right_mm,
                            pupil_distance_left, pupil_distance_right,
                            pupil_center_x_left, pupil_center_y_left, pupil_center_x_right, pupil_center_y_right,
                            blank_left, blank_right, openness_left, openness_right,
                            gaze_point_left_x, gaze_point_left_y, gaze_point_right_x, gaze_point_right_y,
                            gaze_origin_left_x, gaze_origin_left_y, gaze_origin_left_z,
                            gaze_origin_right_x, gaze_origin_right_y, gaze_origin_right_z,
                            gaze_direction_left_x, gaze_direction_left_y, gaze_direction_left_z,
                            gaze_direction_right_x, gaze_direction_right_y, gaze_direction_right_z,
                            fixation_duration_us, fixation_saccade_count, fixation_saccade_state,
                            fixation_saccade_center_x, fixation_saccade_center_y
                        ) VALUES (
                            ?, ?, ?, ?, ?,
                            ?, ?, ?, ?,
                            ?, ?,
                            ?, ?, ?, ?,
                            ?, ?, ?, ?,
                            ?, ?, ?, ?,
                            ?, ?, ?,
                            ?, ?, ?,
                            ?, ?, ?,
                            ?, ?, ?,
                            ?, ?, ?,
                            ?, ?
                        );
                        """, (preparedStatement, eyeTracking) -> {
                    preparedStatement.setLong(1, eyeTracking.getPilotId());
                    preparedStatement.setLong(2, eyeTracking.getTaskId());
                    preparedStatement.setLong(3, eyeTracking.getDeviceId());
                    preparedStatement.setTimestamp(4, eyeTracking.getTimestamp() == null ?
                            null : java.sql.Timestamp.valueOf(eyeTracking.getTimestamp()));
                    preparedStatement.setLong(5, importId);
                    preparedStatement.setObject(6, eyeTracking.getPupilDiameterLeftPx());
                    preparedStatement.setObject(7, eyeTracking.getPupilDiameterLeftMm());
                    preparedStatement.setObject(8, eyeTracking.getPupilDiameterRightPx());
                    preparedStatement.setObject(9, eyeTracking.getPupilDiameterRightMm());
                    preparedStatement.setObject(10, eyeTracking.getPupilDistanceLeft());
                    preparedStatement.setObject(11, eyeTracking.getPupilDistanceRight());
                    preparedStatement.setObject(12, eyeTracking.getPupilCenterXLeft());
                    preparedStatement.setObject(13, eyeTracking.getPupilCenterYLeft());
                    preparedStatement.setObject(14, eyeTracking.getPupilCenterXRight());
                    preparedStatement.setObject(15, eyeTracking.getPupilCenterYRight());
                    preparedStatement.setObject(16, eyeTracking.getBlankLeft());
                    preparedStatement.setObject(17, eyeTracking.getBlankRight());
                    preparedStatement.setObject(18, eyeTracking.getOpennessLeft());
                    preparedStatement.setObject(19, eyeTracking.getOpennessRight());
                    preparedStatement.setObject(20, eyeTracking.getGazePointLeftX());
                    preparedStatement.setObject(21, eyeTracking.getGazePointLeftY());
                    preparedStatement.setObject(22, eyeTracking.getGazePointRightX());
                    preparedStatement.setObject(23, eyeTracking.getGazePointRightY());
                    preparedStatement.setObject(24, eyeTracking.getGazeOriginLeftX());
                    preparedStatement.setObject(25, eyeTracking.getGazeOriginLeftY());
                    preparedStatement.setObject(26, eyeTracking.getGazeOriginLeftZ());
                    preparedStatement.setObject(27, eyeTracking.getGazeOriginRightX());
                    preparedStatement.setObject(28, eyeTracking.getGazeOriginRightY());
                    preparedStatement.setObject(29, eyeTracking.getGazeOriginRightZ());
                    preparedStatement.setObject(30, eyeTracking.getGazeDirectionLeftX());
                    preparedStatement.setObject(31, eyeTracking.getGazeDirectionLeftY());
                    preparedStatement.setObject(32, eyeTracking.getGazeDirectionLeftZ());
                    preparedStatement.setObject(33, eyeTracking.getGazeDirectionRightX());
                    preparedStatement.setObject(34, eyeTracking.getGazeDirectionRightY());
                    preparedStatement.setObject(35, eyeTracking.getGazeDirectionRightZ());
                    preparedStatement.setObject(36, eyeTracking.getFixationDurationUs());
                    preparedStatement.setObject(37, eyeTracking.getFixationSaccadeCount());
                    preparedStatement.setObject(38, eyeTracking.getFixationSaccadeState());
                    preparedStatement.setObject(39, eyeTracking.getFixationSaccadeCenterX());
                    preparedStatement.setObject(40, eyeTracking.getFixationSaccadeCenterY());
                })
                .withExecutionOptions(JdbcSinkUtil.getTiDBJdbcExecutionOptions())
                .buildAtLeastOnce(JdbcSinkUtil.getTiDBJdbcConnectionOptions(TiDBDatabase.PHYSIOLOGICAL));
    }

    @Override
    protected String getReceiverName() {
        return "EyeTracking";
    }

    @Override
    public void start() {
        StreamExecutionEnvironment env = DataReceiverUtil.prepareStreamEnv();
        env.setParallelism(1);
        DataStreamSource<EyeTracking> kafkaSourceDS =
                DataReceiverUtil.getKafkaSourceDS(env, List.of(config.getTopic()), EyeTracking.class);

        // 3. jdbcsink - 使用静态方法避免捕获外部实例
        Sink<EyeTracking> sink = createSink(config.getImportId());

        kafkaSourceDS
                .sinkTo(sink).name("EyeTracking Kafka Sinker");

        try {
            env.execute("EyeTracking Kafka Receiver");
        } catch (Exception e) {
            throw new ZorathosException(e, "Encounter error when executing EyeTrackingKafkaReceiver.");
        }
    }
}
