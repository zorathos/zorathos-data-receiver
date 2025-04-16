package org.datacenter.receiver.physiological;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.jdbc.sink.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.datacenter.config.receiver.physiological.PhysiologicalKafkaReceiverConfig;
import org.datacenter.exception.ZorathosException;
import org.datacenter.model.base.TiDBDatabase;
import org.datacenter.model.physiological.EyeMovement;
import org.datacenter.receiver.BaseReceiver;
import org.datacenter.receiver.util.DataReceiverUtil;
import org.datacenter.receiver.util.JdbcSinkUtil;

import java.util.List;

import static org.datacenter.config.keys.HumanMachineReceiverConfigKey.PHYSIOLOGY_KAFKA_TOPIC;

/**
 * @author : [wangminan]
 * @description : 眼动数据接收
 */
@Slf4j
@AllArgsConstructor
public class EyeMovementKafkaReceiver extends BaseReceiver {

    private PhysiologicalKafkaReceiverConfig config;

    @Override
    public void prepare() {
        super.prepare();
    }

    @Override
    public void start() {
        StreamExecutionEnvironment env = DataReceiverUtil.prepareStreamEnv();
        DataStreamSource<EyeMovement> kafkaSourceDS = DataReceiverUtil.getKafkaSourceDS(env, List.of(config.getTopic()), EyeMovement.class);

        Sink<EyeMovement> sinkFunction = JdbcSink.<EyeMovement>builder()
                .withQueryStatement("""
                        INSERT INTO `physiological`.`eye_movement` (
                            `task_id`, `sensor_id`, `sample_timestamp`,
                            `pupil_diameter_left_px`, `pupil_diameter_left_mm`, `pupil_diameter_right_px`, `pupil_diameter_right_mm`,\s
                            `pupil_distance_left`, `pupil_distance_right`,
                            `pupil_center_x_left`, `pupil_center_y_left`, `pupil_center_x_right`, `pupil_center_y_right`,
                            `blank_left`, `blank_right`,
                            `openness_left`, `openness_right`,
                            `gaze_point_left_x`, `gaze_point_left_y`, `gaze_point_right_x`, `gaze_point_right_y`,
                            `gaze_origin_left_x`, `gaze_origin_left_y`, `gaze_origin_left_z`,
                            `gaze_origin_right_x`, `gaze_origin_right_y`, `gaze_origin_right_z`,
                            `gaze_direction_left_x`, `gaze_direction_left_y`, `gaze_direction_left_z`,
                            `gaze_direction_right_x`, `gaze_direction_right_y`, `gaze_direction_right_z`,
                            `fixation_duration`, `fixation_saccade_count`, `fixation_saccade_state`,
                            `fixation_saccade_center_x`, `fixation_saccade_center_y`
                        ) VALUES (
                            ?, ?, ?,
                            ?, ?, ?, ?,
                            ?, ?,
                            ?, ?, ?, ?,
                            ?, ?,
                            ?, ?,
                            ?, ?, ?, ?,
                            ?, ?, ?,
                            ?, ?, ?,
                            ?, ?, ?,
                            ?, ?, ?,
                            ?, ?, ?,
                            ?, ?
                        );
                        """, (JdbcStatementBuilder<EyeMovement>) (prepareStatement, eyemovement) -> {
                    prepareStatement.setLong(1, eyemovement.getTaskId());
                    prepareStatement.setLong(2, eyemovement.getSensorId());
                    prepareStatement.setLong(3, eyemovement.getSampleTimestamp());
                    prepareStatement.setFloat(4, eyemovement.getPupilDiameterLeftPx());
                    prepareStatement.setFloat(5, eyemovement.getPupilDiameterLeftMm());
                    prepareStatement.setFloat(6, eyemovement.getPupilDiameterRightPx());
                    prepareStatement.setFloat(7, eyemovement.getPupilDiameterRightMm());
                    prepareStatement.setFloat(8, eyemovement.getPupilDistanceLeft());
                    prepareStatement.setFloat(9, eyemovement.getPupilDistanceRight());
                    prepareStatement.setFloat(10, eyemovement.getPupilCenterXLeft());
                    prepareStatement.setFloat(11, eyemovement.getPupilCenterYLeft());
                    prepareStatement.setFloat(12, eyemovement.getPupilCenterXRight());
                    prepareStatement.setFloat(13, eyemovement.getPupilCenterYRight());
                    prepareStatement.setFloat(14, eyemovement.getBlankLeft());
                    prepareStatement.setFloat(15, eyemovement.getBlankRight());
                    prepareStatement.setFloat(16, eyemovement.getOpennessLeft());
                    prepareStatement.setFloat(17, eyemovement.getOpennessRight());
                    prepareStatement.setFloat(18, eyemovement.getGazePointLeftX());
                    prepareStatement.setFloat(19, eyemovement.getGazePointLeftY());
                    prepareStatement.setFloat(20, eyemovement.getGazePointRightX());
                    prepareStatement.setFloat(21, eyemovement.getGazePointRightY());
                    prepareStatement.setFloat(22, eyemovement.getGazeOriginLeftX());
                    prepareStatement.setFloat(23, eyemovement.getGazeOriginLeftY());
                    prepareStatement.setFloat(24, eyemovement.getGazeOriginLeftZ());
                    prepareStatement.setFloat(25, eyemovement.getGazeOriginRightX());
                    prepareStatement.setFloat(26, eyemovement.getGazeOriginRightY());
                    prepareStatement.setFloat(27, eyemovement.getGazeOriginRightZ());
                    prepareStatement.setFloat(28, eyemovement.getGazeDirectionLeftX());
                    prepareStatement.setFloat(29, eyemovement.getGazeDirectionLeftY());
                    prepareStatement.setFloat(30, eyemovement.getGazeDirectionLeftZ());
                    prepareStatement.setFloat(31, eyemovement.getGazeDirectionRightX());
                    prepareStatement.setFloat(32, eyemovement.getGazeDirectionRightY());
                    prepareStatement.setFloat(33, eyemovement.getGazeDirectionRightZ());
                    prepareStatement.setFloat(34, eyemovement.getFixationDuration());
                    prepareStatement.setFloat(35, eyemovement.getFixationSaccadeCount());
                    prepareStatement.setFloat(36, eyemovement.getFixationSaccadeState());
                    prepareStatement.setFloat(37, eyemovement.getFixationSaccadeCenterX());
                    prepareStatement.setFloat(38, eyemovement.getFixationSaccadeCenterY());
                })
                .withExecutionOptions(JdbcSinkUtil.getTiDBJdbcExecutionOptions())
                .buildAtLeastOnce(JdbcSinkUtil.getTiDBJdbcConnectionOptions(TiDBDatabase.PHYSIOLOGICAL));

        kafkaSourceDS.sinkTo(sinkFunction).name("EyeMovement Kafka Sinker");

        try {
            env.execute("EyeMovement Kafka Receiver");
        } catch (Exception e) {
            throw new ZorathosException(e, "Encounter error when executing EyeMovementKafkaReceiver.");
        }
    }

    /**
     * 主函数
     *
     * @param args 接收器参数 入参为 --topic xxxx
     */
    public static void main(String[] args) {
        ParameterTool params = ParameterTool.fromArgs(args);
        String topic = params.getRequired(PHYSIOLOGY_KAFKA_TOPIC.getKeyForParamsMap());
        PhysiologicalKafkaReceiverConfig config = PhysiologicalKafkaReceiverConfig.builder()
                .topic(topic)
                .build();
        EyeMovementKafkaReceiver receiver = new EyeMovementKafkaReceiver(config);
        receiver.run();
    }
}
