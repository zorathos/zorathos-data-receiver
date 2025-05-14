package org.datacenter.receiver.physiological.input;

import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.jdbc.sink.JdbcSink;
import org.datacenter.config.receiver.physiological.PhysiologicalKafkaReceiverConfig;
import org.datacenter.model.base.TiDBDatabase;
import org.datacenter.model.physiological.input.EegEmg;
import org.datacenter.receiver.physiological.base.PhysiologicalKafkaReceiver;
import org.datacenter.receiver.util.JdbcSinkUtil;

/**
 * @author : [wangminan]
 * @description : 脑电肌电数据在线接收器
 */
@EqualsAndHashCode(callSuper = true)
@Slf4j
public class EegEmgKafkaReceiver extends PhysiologicalKafkaReceiver<EegEmg> {

    public static void main(String[] args) {
        PhysiologicalKafkaReceiverConfig config = parseArgs(args);
        EegEmgKafkaReceiver receiver = new EegEmgKafkaReceiver();
        receiver.setConfig(config);
        receiver.run();
    }

    @Override
    protected Sink<EegEmg> createSink(Long importId) {
        return JdbcSink.<EegEmg>builder()
                .withQueryStatement("""
                        INSERT INTO eeg_emg (
                            record_id, task_id, device_id, timestamp, sampling_rate, import_id,
                            channel_1, channel_2, channel_3, channel_4, channel_5,
                            channel_6, channel_7, channel_8, channel_9, channel_10,
                            emg_1, emg_2, emg_3, emg_4, emg_5, emg_6, emg_7, emg_8
                        ) VALUES (
                            ?, ?, ?, ?, ?, ?,
                            ?, ?, ?, ?, ?,
                            ?, ?, ?, ?, ?,
                            ?, ?, ?, ?, ?, ?, ?, ?
                        );
                        """, (preparedStatement, eegEmg) -> {
                    preparedStatement.setLong(1, eegEmg.getRecordId());
                    preparedStatement.setLong(2, eegEmg.getTaskId());
                    preparedStatement.setLong(3, eegEmg.getDeviceId());
                    preparedStatement.setTimestamp(4, eegEmg.getTimestamp() == null ?
                            null : java.sql.Timestamp.valueOf(eegEmg.getTimestamp()));
                    preparedStatement.setDouble(5, eegEmg.getSamplingRate());
                    preparedStatement.setLong(6, importId);
                    preparedStatement.setObject(7, eegEmg.getChannel1());
                    preparedStatement.setObject(8, eegEmg.getChannel2());
                    preparedStatement.setObject(9, eegEmg.getChannel3());
                    preparedStatement.setObject(10, eegEmg.getChannel4());
                    preparedStatement.setObject(11, eegEmg.getChannel5());
                    preparedStatement.setObject(12, eegEmg.getChannel6());
                    preparedStatement.setObject(13, eegEmg.getChannel7());
                    preparedStatement.setObject(14, eegEmg.getChannel8());
                    preparedStatement.setObject(15, eegEmg.getChannel9());
                    preparedStatement.setObject(16, eegEmg.getChannel10());
                    preparedStatement.setObject(17, eegEmg.getEmg1());
                    preparedStatement.setObject(18, eegEmg.getEmg2());
                    preparedStatement.setObject(19, eegEmg.getEmg3());
                    preparedStatement.setObject(20, eegEmg.getEmg4());
                    preparedStatement.setObject(21, eegEmg.getEmg5());
                    preparedStatement.setObject(22, eegEmg.getEmg6());
                    preparedStatement.setObject(23, eegEmg.getEmg7());
                    preparedStatement.setObject(24, eegEmg.getEmg8());
                })
                .withExecutionOptions(JdbcSinkUtil.getTiDBJdbcExecutionOptions())
                .buildAtLeastOnce(JdbcSinkUtil.getTiDBJdbcConnectionOptions(TiDBDatabase.PHYSIOLOGICAL));
    }

    @Override
    protected String getReceiverName() {
        return "EegEmg";
    }

    @Override
    protected Class<EegEmg> getDataClass() {
        return EegEmg.class;
    }
}
