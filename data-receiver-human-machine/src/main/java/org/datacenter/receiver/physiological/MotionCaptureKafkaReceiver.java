package org.datacenter.receiver.physiological;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.jdbc.sink.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.datacenter.config.physiological.PhysiologicalKafkaReceiverConfig;
import org.datacenter.model.base.TiDBDatabase;
import org.datacenter.model.physiological.MotionCapture;
import org.datacenter.receiver.BaseReceiver;
import org.datacenter.receiver.util.DataReceiverUtil;
import org.datacenter.receiver.util.JdbcSinkUtil;

import java.sql.Timestamp;
import java.util.List;

/**
 * @author : [wangminan]
 * @description : 三维动捕数据Kafka接收器
 */
@Slf4j
@AllArgsConstructor
public class MotionCaptureKafkaReceiver extends BaseReceiver {

    private PhysiologicalKafkaReceiverConfig config;

    @Override
    public void prepare() {
        super.prepare();
    }

    @Override
    public void start() {
        StreamExecutionEnvironment env = DataReceiverUtil.prepareStreamEnv();
        DataStreamSource<MotionCapture> kafkaSourceDS = DataReceiverUtil.getKafkaSourceDS(env, List.of(config.getTopic()), MotionCapture.class);

        Sink<MotionCapture> sinkFunction = JdbcSink.<MotionCapture>builder()
                .withQueryStatement("""
                        INSERT INTO `physiological`.`motion_capture` (
                            `sortie_number`, `sensor_id`, `sample_timestamp`,
                            `hips_qx`, `hips_qy`, `hips_qz`, `hips_px`, `hips_py`, `hips_pz`, `hips_timestamp`,
                            `joint_tag_spine_qx`, `joint_tag_spine_qy`, `joint_tag_spine_qz`, `joint_tag_spine_px`, `joint_tag_spine_py`, `joint_tag_spine_pz`, `joint_tag_spine_timestamp`,
                            `joint_tag_spine1_qx`, `joint_tag_spine1_qy`, `joint_tag_spine1_qz`, `joint_tag_spine1_px`, `joint_tag_spine1_py`, `joint_tag_spine1_pz`, `joint_tag_spine1_timestamp`,
                            `joint_tag_spine2_qx`, `joint_tag_spine2_qy`, `joint_tag_spine2_qz`, `joint_tag_spine2_px`, `joint_tag_spine2_py`, `joint_tag_spine2_pz`, `joint_tag_spine2_timestamp`,
                            `joint_tag_neck_qx`, `joint_tag_neck_qy`, `joint_tag_neck_qz`, `joint_tag_neck_px`, `joint_tag_neck_py`, `joint_tag_neck_pz`, `joint_tag_neck_timestamp`,
                            `joint_tag_neck1_qx`, `joint_tag_neck1_qy`, `joint_tag_neck1_qz`, `joint_tag_neck1_px`, `joint_tag_neck1_py`, `joint_tag_neck1_pz`, `joint_tag_neck1_timestamp`,
                            `joint_tag_head_qx`, `joint_tag_head_qy`, `joint_tag_head_qz`, `joint_tag_head_px`, `joint_tag_head_py`, `joint_tag_head_pz`, `joint_tag_head_timestamp`,
                            `joint_tag_right_shoulder_qx`, `joint_tag_right_shoulder_qy`, `joint_tag_right_shoulder_qz`, `joint_tag_right_shoulder_px`, `joint_tag_right_shoulder_py`, `joint_tag_right_shoulder_pz`, `joint_tag_right_shoulder_timestamp`,
                            `joint_tag_right_arm_qx`, `joint_tag_right_arm_qy`, `joint_tag_right_arm_qz`, `joint_tag_right_arm_px`, `joint_tag_right_arm_py`, `joint_tag_right_arm_pz`, `joint_tag_right_arm_timestamp`,
                            `joint_tag_right_fore_arm_qx`, `joint_tag_right_fore_arm_qy`, `joint_tag_right_fore_arm_qz`, `joint_tag_right_fore_arm_px`, `joint_tag_right_fore_arm_py`, `joint_tag_right_fore_arm_pz`, `joint_tag_right_fore_arm_timestamp`,
                            `joint_tag_right_hand_qx`, `joint_tag_right_hand_qy`, `joint_tag_right_hand_qz`, `joint_tag_right_hand_px`, `joint_tag_right_hand_py`, `joint_tag_right_hand_pz`, `joint_tag_right_hand_timestamp`,
                            `joint_tag_right_in_hand_index_qx`, `joint_tag_right_in_hand_index_qy`, `joint_tag_right_in_hand_index_qz`, `joint_tag_right_in_hand_index_px`, `joint_tag_right_in_hand_index_py`, `joint_tag_right_in_hand_index_pz`, `joint_tag_right_in_hand_index_timestamp`,
                            `joint_tag_right_in_hand_middle_qx`, `joint_tag_right_in_hand_middle_qy`, `joint_tag_right_in_hand_middle_qz`, `joint_tag_right_in_hand_middle_px`, `joint_tag_right_in_hand_middle_py`, `joint_tag_right_in_hand_middle_pz`, `joint_tag_right_in_hand_middle_timestamp`,
                            `joint_tag_left_shoulder_qx`, `joint_tag_left_shoulder_qy`, `joint_tag_left_shoulder_qz`, `joint_tag_left_shoulder_px`, `joint_tag_left_shoulder_py`, `joint_tag_left_shoulder_pz`, `joint_tag_left_shoulder_timestamp`,
                            `joint_tag_left_arm_qx`, `joint_tag_left_arm_qy`, `joint_tag_left_arm_qz`, `joint_tag_left_arm_px`, `joint_tag_left_arm_py`, `joint_tag_left_arm_pz`, `joint_tag_left_arm_timestamp`,
                            `joint_tag_left_fore_arm_qx`, `joint_tag_left_fore_arm_qy`, `joint_tag_left_fore_arm_qz`, `joint_tag_left_fore_arm_px`, `joint_tag_left_fore_arm_py`, `joint_tag_left_fore_arm_pz`, `joint_tag_left_fore_arm_timestamp`,
                            `joint_tag_left_hand_qx`, `joint_tag_left_hand_qy`, `joint_tag_left_hand_qz`, `joint_tag_left_hand_px`, `joint_tag_left_hand_py`, `joint_tag_left_hand_pz`, `joint_tag_left_hand_timestamp`,
                            `joint_tag_left_in_hand_index_qx`, `joint_tag_left_in_hand_index_qy`, `joint_tag_left_in_hand_index_qz`, `joint_tag_left_in_hand_index_px`, `joint_tag_left_in_hand_index_py`, `joint_tag_left_in_hand_index_pz`, `joint_tag_left_in_hand_index_timestamp`,
                            `joint_tag_left_in_hand_middle_qx`, `joint_tag_left_in_hand_middle_qy`, `joint_tag_left_in_hand_middle_qz`, `joint_tag_left_in_hand_middle_px`, `joint_tag_left_in_hand_middle_py`, `joint_tag_left_in_hand_middle_pz`, `joint_tag_left_in_hand_middle_timestamp`
                        ) VALUES (
                            ?, ?, ?, ?,
                            ?, ?, ?, ?, ?, ?, ?,
                            ?, ?, ?, ?, ?, ?, ?,
                            ?, ?, ?, ?, ?, ?, ?,
                            ?, ?, ?, ?, ?, ?, ?,
                            ?, ?, ?, ?, ?, ?, ?,
                            ?, ?, ?, ?, ?, ?, ?,
                            ?, ?, ?, ?, ?, ?, ?,
                            ?, ?, ?, ?, ?, ?, ?,
                            ?, ?, ?, ?, ?, ?, ?,
                            ?, ?, ?, ?, ?, ?, ?,
                            ?, ?, ?, ?, ?, ?, ?,
                            ?, ?, ?, ?, ?, ?, ?,
                            ?, ?, ?, ?, ?, ?, ?,
                            ?, ?, ?, ?, ?, ?, ?,
                            ?, ?, ?, ?, ?, ?, ?,
                            ?, ?, ?, ?, ?, ?, ?,
                            ?, ?, ?, ?, ?, ?, ?,
                            ?, ?, ?, ?, ?, ?, ?
                        );
                        """, (JdbcStatementBuilder<MotionCapture>) (preparedStatement, motionCapture) -> {
                    preparedStatement.setString(1, config.getSortieNumber());
                    preparedStatement.setString(2, motionCapture.getSensorId());
                    preparedStatement.setTimestamp(3, motionCapture.getSampleTimestamp() == null ?
                            null : Timestamp.valueOf(motionCapture.getSampleTimestamp()));
                    preparedStatement.setFloat(4, motionCapture.getHipsQx());
                    preparedStatement.setFloat(5, motionCapture.getHipsQy());
                    preparedStatement.setFloat(6, motionCapture.getHipsQz());
                    preparedStatement.setFloat(7, motionCapture.getHipsPx());
                    preparedStatement.setFloat(8, motionCapture.getHipsPy());
                    preparedStatement.setFloat(9, motionCapture.getHipsPz());
                    preparedStatement.setTimestamp(10, motionCapture.getHipsTimestamp() == null ?
                            null : Timestamp.valueOf(motionCapture.getHipsTimestamp()));
                    preparedStatement.setFloat(11, motionCapture.getJointTagSpineQx());
                    preparedStatement.setFloat(12, motionCapture.getJointTagSpineQy());
                    preparedStatement.setFloat(13, motionCapture.getJointTagSpineQz());
                    preparedStatement.setFloat(14, motionCapture.getJointTagSpinePx());
                    preparedStatement.setFloat(15, motionCapture.getJointTagSpinePy());
                    preparedStatement.setFloat(16, motionCapture.getJointTagSpinePz());
                    preparedStatement.setTimestamp(17, motionCapture.getJointTagSpineTimestamp() == null ?
                            null : Timestamp.valueOf(motionCapture.getJointTagSpineTimestamp()));
                    preparedStatement.setFloat(18, motionCapture.getJointTagSpine1Qx());
                    preparedStatement.setFloat(19, motionCapture.getJointTagSpine1Qy());
                    preparedStatement.setFloat(20, motionCapture.getJointTagSpine1Qz());
                    preparedStatement.setFloat(21, motionCapture.getJointTagSpine1Px());
                    preparedStatement.setFloat(22, motionCapture.getJointTagSpine1Py());
                    preparedStatement.setFloat(23, motionCapture.getJointTagSpine1Pz());
                    preparedStatement.setTimestamp(24, motionCapture.getJointTagSpine1Timestamp() == null ?
                            null : Timestamp.valueOf(motionCapture.getJointTagSpine1Timestamp()));
                    preparedStatement.setFloat(25, motionCapture.getJointTagSpine2Qx());
                    preparedStatement.setFloat(26, motionCapture.getJointTagSpine2Qy());
                    preparedStatement.setFloat(27, motionCapture.getJointTagSpine2Qz());
                    preparedStatement.setFloat(28, motionCapture.getJointTagSpine2Px());
                    preparedStatement.setFloat(29, motionCapture.getJointTagSpine2Py());
                    preparedStatement.setFloat(30, motionCapture.getJointTagSpine2Pz());
                    preparedStatement.setTimestamp(31, motionCapture.getJointTagSpine2Timestamp() == null ?
                            null : Timestamp.valueOf(motionCapture.getJointTagSpine2Timestamp()));
                    preparedStatement.setFloat(32, motionCapture.getJointTagNeckQx());
                    preparedStatement.setFloat(33, motionCapture.getJointTagNeckQy());
                    preparedStatement.setFloat(34, motionCapture.getJointTagNeckQz());
                    preparedStatement.setFloat(35, motionCapture.getJointTagNeckPx());
                    preparedStatement.setFloat(36, motionCapture.getJointTagNeckPy());
                    preparedStatement.setFloat(37, motionCapture.getJointTagNeckPz());
                    preparedStatement.setTimestamp(38, motionCapture.getJointTagNeckTimestamp() == null ?
                            null : Timestamp.valueOf(motionCapture.getJointTagNeckTimestamp()));
                    preparedStatement.setFloat(39, motionCapture.getJointTagNeck1Qx());
                    preparedStatement.setFloat(40, motionCapture.getJointTagNeck1Qy());
                    preparedStatement.setFloat(41, motionCapture.getJointTagNeck1Qz());
                    preparedStatement.setFloat(42, motionCapture.getJointTagNeck1Px());
                    preparedStatement.setFloat(43, motionCapture.getJointTagNeck1Py());
                    preparedStatement.setFloat(44, motionCapture.getJointTagNeck1Pz());
                    preparedStatement.setTimestamp(45, motionCapture.getJointTagNeck1Timestamp() == null ?
                            null : Timestamp.valueOf(motionCapture.getJointTagNeck1Timestamp()));
                    preparedStatement.setFloat(46, motionCapture.getJointTagHeadQx());
                    preparedStatement.setFloat(47, motionCapture.getJointTagHeadQy());
                    preparedStatement.setFloat(48, motionCapture.getJointTagHeadQz());
                    preparedStatement.setFloat(49, motionCapture.getJointTagHeadPx());
                    preparedStatement.setFloat(50, motionCapture.getJointTagHeadPy());
                    preparedStatement.setFloat(51, motionCapture.getJointTagHeadPz());
                    preparedStatement.setTimestamp(52, motionCapture.getJointTagHeadTimestamp() == null ?
                            null : Timestamp.valueOf(motionCapture.getJointTagHeadTimestamp()));
                    preparedStatement.setFloat(53, motionCapture.getJointTagRightShoulderQx());
                    preparedStatement.setFloat(54, motionCapture.getJointTagRightShoulderQy());
                    preparedStatement.setFloat(55, motionCapture.getJointTagRightShoulderQz());
                    preparedStatement.setFloat(56, motionCapture.getJointTagRightShoulderPx());
                    preparedStatement.setFloat(57, motionCapture.getJointTagRightShoulderPy());
                    preparedStatement.setFloat(58, motionCapture.getJointTagRightShoulderPz());
                    preparedStatement.setTimestamp(59, motionCapture.getJointTagRightShoulderTimestamp() == null ?
                            null : Timestamp.valueOf(motionCapture.getJointTagRightShoulderTimestamp()));
                    preparedStatement.setFloat(60, motionCapture.getJointTagRightArmQx());
                    preparedStatement.setFloat(61, motionCapture.getJointTagRightArmQy());
                    preparedStatement.setFloat(62, motionCapture.getJointTagRightArmQz());
                    preparedStatement.setFloat(63, motionCapture.getJointTagRightArmPx());
                    preparedStatement.setFloat(64, motionCapture.getJointTagRightArmPy());
                    preparedStatement.setFloat(65, motionCapture.getJointTagRightArmPz());
                    preparedStatement.setTimestamp(66, motionCapture.getJointTagRightArmTimestamp() == null ?
                            null : Timestamp.valueOf(motionCapture.getJointTagRightArmTimestamp()));
                    preparedStatement.setFloat(67, motionCapture.getJointTagRightForeArmQx());
                    preparedStatement.setFloat(68, motionCapture.getJointTagRightForeArmQy());
                    preparedStatement.setFloat(69, motionCapture.getJointTagRightForeArmQz());
                    preparedStatement.setFloat(70, motionCapture.getJointTagRightForeArmPx());
                    preparedStatement.setFloat(71, motionCapture.getJointTagRightForeArmPy());
                    preparedStatement.setFloat(72, motionCapture.getJointTagRightForeArmPz());
                    preparedStatement.setTimestamp(73, motionCapture.getJointTagRightForeArmTimestamp() == null ?
                            null : Timestamp.valueOf(motionCapture.getJointTagRightForeArmTimestamp()));
                    preparedStatement.setFloat(74, motionCapture.getJointTagRightHandQx());
                    preparedStatement.setFloat(75, motionCapture.getJointTagRightHandQy());
                    preparedStatement.setFloat(76, motionCapture.getJointTagRightHandQz());
                    preparedStatement.setFloat(77, motionCapture.getJointTagRightHandPx());
                    preparedStatement.setFloat(78, motionCapture.getJointTagRightHandPy());
                    preparedStatement.setFloat(79, motionCapture.getJointTagRightHandPz());
                    preparedStatement.setTimestamp(80, motionCapture.getJointTagRightHandTimestamp() == null ?
                            null : Timestamp.valueOf(motionCapture.getJointTagRightHandTimestamp()));
                    preparedStatement.setFloat(81, motionCapture.getJointTagRightInHandIndexQx());
                    preparedStatement.setFloat(82, motionCapture.getJointTagRightInHandIndexQy());
                    preparedStatement.setFloat(83, motionCapture.getJointTagRightInHandIndexQz());
                    preparedStatement.setFloat(84, motionCapture.getJointTagRightInHandIndexPx());
                    preparedStatement.setFloat(85, motionCapture.getJointTagRightInHandIndexPy());
                    preparedStatement.setFloat(86, motionCapture.getJointTagRightInHandIndexPz());
                    preparedStatement.setTimestamp(87, motionCapture.getJointTagRightInHandIndexTimestamp() == null ?
                            null : Timestamp.valueOf(motionCapture.getJointTagRightInHandIndexTimestamp()));
                    preparedStatement.setFloat(88, motionCapture.getJointTagRightInHandMiddleQx());
                    preparedStatement.setFloat(89, motionCapture.getJointTagRightInHandMiddleQy());
                    preparedStatement.setFloat(90, motionCapture.getJointTagRightInHandMiddleQz());
                    preparedStatement.setFloat(91, motionCapture.getJointTagRightInHandMiddlePx());
                    preparedStatement.setFloat(92, motionCapture.getJointTagRightInHandMiddlePy());
                    preparedStatement.setFloat(93, motionCapture.getJointTagRightInHandMiddlePz());
                    preparedStatement.setTimestamp(94, motionCapture.getJointTagRightInHandMiddleTimestamp() == null ?
                            null : Timestamp.valueOf(motionCapture.getJointTagRightInHandMiddleTimestamp()));
                    preparedStatement.setFloat(95, motionCapture.getJointTagLeftShoulderQx());
                    preparedStatement.setFloat(96, motionCapture.getJointTagLeftShoulderQy());
                    preparedStatement.setFloat(97, motionCapture.getJointTagLeftShoulderQz());
                    preparedStatement.setFloat(98, motionCapture.getJointTagLeftShoulderPx());
                    preparedStatement.setFloat(99, motionCapture.getJointTagLeftShoulderPy());
                    preparedStatement.setFloat(100, motionCapture.getJointTagLeftShoulderPz());
                    preparedStatement.setTimestamp(101, motionCapture.getJointTagLeftShoulderTimestamp() == null ?
                            null : Timestamp.valueOf(motionCapture.getJointTagLeftShoulderTimestamp()));
                    preparedStatement.setFloat(102, motionCapture.getJointTagLeftArmQx());
                    preparedStatement.setFloat(103, motionCapture.getJointTagLeftArmQy());
                    preparedStatement.setFloat(104, motionCapture.getJointTagLeftArmQz());
                    preparedStatement.setFloat(105, motionCapture.getJointTagLeftArmPx());
                    preparedStatement.setFloat(106, motionCapture.getJointTagLeftArmPy());
                    preparedStatement.setFloat(107, motionCapture.getJointTagLeftArmPz());
                    preparedStatement.setTimestamp(108, motionCapture.getJointTagLeftArmTimestamp() == null ?
                            null : Timestamp.valueOf(motionCapture.getJointTagLeftArmTimestamp()));
                    preparedStatement.setFloat(109, motionCapture.getJointTagLeftForeArmQx());
                    preparedStatement.setFloat(110, motionCapture.getJointTagLeftForeArmQy());
                    preparedStatement.setFloat(111, motionCapture.getJointTagLeftForeArmQz());
                    preparedStatement.setFloat(112, motionCapture.getJointTagLeftForeArmPx());
                    preparedStatement.setFloat(113, motionCapture.getJointTagLeftForeArmPy());
                    preparedStatement.setFloat(114, motionCapture.getJointTagLeftForeArmPz());
                    preparedStatement.setTimestamp(115, motionCapture.getJointTagLeftForeArmTimestamp() == null ?
                            null : Timestamp.valueOf(motionCapture.getJointTagLeftForeArmTimestamp()));
                    preparedStatement.setFloat(116, motionCapture.getJointTagLeftHandQx());
                    preparedStatement.setFloat(117, motionCapture.getJointTagLeftHandQy());
                    preparedStatement.setFloat(118, motionCapture.getJointTagLeftHandQz());
                    preparedStatement.setFloat(119, motionCapture.getJointTagLeftHandPx());
                    preparedStatement.setFloat(120, motionCapture.getJointTagLeftHandPy());
                    preparedStatement.setFloat(121, motionCapture.getJointTagLeftHandPz());
                    preparedStatement.setTimestamp(122, motionCapture.getJointTagLeftHandTimestamp() == null ?
                            null : Timestamp.valueOf(motionCapture.getJointTagLeftHandTimestamp()));
                    preparedStatement.setFloat(123, motionCapture.getJointTagLeftInHandIndexQx());
                    preparedStatement.setFloat(124, motionCapture.getJointTagLeftInHandIndexQy());
                    preparedStatement.setFloat(125, motionCapture.getJointTagLeftInHandIndexQz());
                    preparedStatement.setFloat(126, motionCapture.getJointTagLeftInHandIndexPx());
                    preparedStatement.setFloat(127, motionCapture.getJointTagLeftInHandIndexPy());
                    preparedStatement.setFloat(128, motionCapture.getJointTagLeftInHandIndexPz());
                    preparedStatement.setTimestamp(129, motionCapture.getJointTagLeftInHandIndexTimestamp() == null ?
                            null : Timestamp.valueOf(motionCapture.getJointTagLeftInHandIndexTimestamp()));
                    preparedStatement.setFloat(130, motionCapture.getJointTagLeftInHandMiddleQx());
                    preparedStatement.setFloat(131, motionCapture.getJointTagLeftInHandMiddleQy());
                    preparedStatement.setFloat(132, motionCapture.getJointTagLeftInHandMiddleQz());
                    preparedStatement.setFloat(133, motionCapture.getJointTagLeftInHandMiddlePx());
                    preparedStatement.setFloat(134, motionCapture.getJointTagLeftInHandMiddlePy());
                    preparedStatement.setFloat(135, motionCapture.getJointTagLeftInHandMiddlePz());
                    preparedStatement.setTimestamp(136, motionCapture.getJointTagLeftInHandMiddleTimestamp() == null ?
                            null : Timestamp.valueOf(motionCapture.getJointTagLeftInHandMiddleTimestamp()));
                })
                .withExecutionOptions(JdbcSinkUtil.getTiDBJdbcExecutionOptions())
                .buildAtLeastOnce(JdbcSinkUtil.getTiDBJdbcConnectionOptions(TiDBDatabase.PHYSIOLOGICAL));

        kafkaSourceDS.sinkTo(sinkFunction).name("MotionCapture Kafka Sinker");
        try {
            env.execute("MotionCapture Kafka Receiver");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 主函数
     * @param args 接收器参数 入参为 --topic xxxx
     */
    public static void main(String[] args) {
        ParameterTool params = ParameterTool.fromArgs(args);
        String topic = params.getRequired("topic");
        PhysiologicalKafkaReceiverConfig config = PhysiologicalKafkaReceiverConfig.builder()
                .topic(topic)
                .build();
        MotionCaptureKafkaReceiver receiver = new MotionCaptureKafkaReceiver(config);
        receiver.run();
    }
}
