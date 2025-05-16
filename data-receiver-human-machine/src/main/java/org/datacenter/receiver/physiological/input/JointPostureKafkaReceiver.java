package org.datacenter.receiver.physiological.input;

import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.jdbc.sink.JdbcSink;
import org.datacenter.config.receiver.physiological.PhysiologicalKafkaReceiverConfig;
import org.datacenter.model.base.TiDBDatabase;
import org.datacenter.model.physiological.input.JointPosture;
import org.datacenter.receiver.physiological.base.PhysiologicalKafkaReceiver;
import org.datacenter.receiver.util.JdbcSinkUtil;

@EqualsAndHashCode(callSuper = true)
@Slf4j
public class JointPostureKafkaReceiver extends PhysiologicalKafkaReceiver<JointPosture> {

    public static void main(String[] args) {
        PhysiologicalKafkaReceiverConfig config = parseArgs(args);
        JointPostureKafkaReceiver receiver = new JointPostureKafkaReceiver();
        // 将接收器设置为接收JSON数组
        receiver.setItemInArray(true);
        receiver.setConfig(config);
        receiver.run();
    }

    @Override
    public void prepare() {
        super.prepare();
        modelClass = JointPosture.class;
    }

    @Override
    protected Sink<JointPosture> createSink(Long importId) {
        return JdbcSink.<JointPosture>builder()
                .withQueryStatement("""
                        INSERT INTO joint_posture (
                            pilot_id, task_id, device_id, timestamp, import_id,
                            joint_name, qx, qy, qz, qw, px, py, pz
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                        """, (preparedStatement, jointPosture) -> {
                    // 绑定数据逻辑保持不变
                    preparedStatement.setLong(1, jointPosture.getPilotId());
                    preparedStatement.setLong(2, jointPosture.getTaskId());
                    preparedStatement.setLong(3, jointPosture.getDeviceId());
                    preparedStatement.setTimestamp(4, jointPosture.getTimestamp() == null ?
                            null : java.sql.Timestamp.valueOf(jointPosture.getTimestamp()));
                    preparedStatement.setLong(5, importId);
                    preparedStatement.setString(6, jointPosture.getJointName() != null ?
                            jointPosture.getJointName().name() : null);
                    preparedStatement.setObject(7, jointPosture.getQx());
                    preparedStatement.setObject(8, jointPosture.getQy());
                    preparedStatement.setObject(9, jointPosture.getQz());
                    preparedStatement.setObject(10, jointPosture.getQw());
                    preparedStatement.setObject(11, jointPosture.getPx());
                    preparedStatement.setObject(12, jointPosture.getPy());
                    preparedStatement.setObject(13, jointPosture.getPz());
                })
                .withExecutionOptions(JdbcSinkUtil.getTiDBJdbcExecutionOptions())
                .buildAtLeastOnce(JdbcSinkUtil.getTiDBJdbcConnectionOptions(TiDBDatabase.PHYSIOLOGICAL));
    }

    @Override
    protected String getReceiverName() {
        return "JointPosture";
    }
}
