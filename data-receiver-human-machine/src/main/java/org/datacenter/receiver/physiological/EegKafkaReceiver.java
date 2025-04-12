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
import org.datacenter.model.physiological.Eeg;
import org.datacenter.receiver.BaseReceiver;
import org.datacenter.receiver.util.DataReceiverUtil;
import org.datacenter.receiver.util.JdbcSinkUtil;

import java.util.List;

import static org.datacenter.config.keys.HumanMachineReceiverConfigKey.PHYSIOLOGY_KAFKA_TOPIC;

/**
 * @author : [wangminan]
 * @description : 脑电数据Kafka接收器
 */
@Slf4j
@AllArgsConstructor
public class EegKafkaReceiver extends BaseReceiver {

    private PhysiologicalKafkaReceiverConfig config;

    @Override
    public void prepare() {
        super.prepare();
    }

    @Override
    public void start() {
        StreamExecutionEnvironment env = DataReceiverUtil.prepareStreamEnv();
        DataStreamSource<Eeg> kafkaSourceDS =
                DataReceiverUtil.getKafkaSourceDS(env, List.of(config.getTopic()), Eeg.class);

        Sink<Eeg> sinkFunction = JdbcSink.<Eeg>builder()
                .withQueryStatement("""
                INSERT INTO `physiological`.`eeg` (
                    `task_id`, `sensor_id`, `sample_timestamp`,
                    `stream_name`, `channel_count`, `sampling_rate`, `channel_format`,
                    `channel_1`, `channel_2`, `channel_3`, `channel_4`,
                    `channel_5`, `channel_6`, `channel_7`, `channel_8`,
                    `channel_9`, `channel_10`, `channel_11`, `channel_12`,
                    `channel_13`, `channel_14`, `channel_15`, `channel_16`
                ) VALUES (
                    ?, ?, ?,?, ?, ?, ?,?, ?, ?, ?,?, ?, ?, ?,?, ?, ?, ?,?, ?, ?, ?
                );
                """,
                    (JdbcStatementBuilder<Eeg>) (preparedStatement, eeg) -> {
                        preparedStatement.setLong(1, eeg.getTaskId());
                        preparedStatement.setLong(2, eeg.getSensorId());
                        preparedStatement.setLong(3, eeg.getSampleTimestamp());
                        preparedStatement.setString(4, eeg.getStreamName());
                        preparedStatement.setInt(5, eeg.getChannelCount());
                        preparedStatement.setDouble(6, eeg.getSamplingRate());
                        preparedStatement.setString(7, eeg.getChannelFormat());
                        preparedStatement.setDouble(8, eeg.getChannel1());
                        preparedStatement.setDouble(9, eeg.getChannel2());
                        preparedStatement.setDouble(10, eeg.getChannel3());
                        preparedStatement.setDouble(11, eeg.getChannel4());
                        preparedStatement.setDouble(12, eeg.getChannel5());
                        preparedStatement.setDouble(13, eeg.getChannel6());
                        preparedStatement.setDouble(14, eeg.getChannel7());
                        preparedStatement.setDouble(15, eeg.getChannel8());
                        preparedStatement.setDouble(16, eeg.getChannel9());
                        preparedStatement.setDouble(17, eeg.getChannel10());
                        preparedStatement.setDouble(18, eeg.getChannel11());
                        preparedStatement.setDouble(19, eeg.getChannel12());
                        preparedStatement.setDouble(20, eeg.getChannel13());
                        preparedStatement.setDouble(21, eeg.getChannel14());
                        preparedStatement.setDouble(22, eeg.getChannel15());
                        preparedStatement.setDouble(23, eeg.getChannel16());
                    }
                )
                .withExecutionOptions(JdbcSinkUtil.getTiDBJdbcExecutionOptions())
                .buildAtLeastOnce(JdbcSinkUtil.getTiDBJdbcConnectionOptions(TiDBDatabase.PHYSIOLOGICAL));

        kafkaSourceDS.sinkTo(sinkFunction).name("Eeg Kafka Sinker");

        try {
            env.execute("Eeg Kafka Receiver");
        } catch (Exception e) {
            throw new ZorathosException(e, "Encounter error when executing EegKafkaReceiver.");
        }
    }

    /**
     * 主函数
     * @param args 接收器参数 入参为 --topic xxxx
     */
    public static void main(String[] args) {
        ParameterTool params = ParameterTool.fromArgs(args);
        String topic = params.getRequired(PHYSIOLOGY_KAFKA_TOPIC.getKeyForParamsMap());
        PhysiologicalKafkaReceiverConfig config = PhysiologicalKafkaReceiverConfig.builder()
                .topic(topic)
                .build();
        EegKafkaReceiver receiver = new EegKafkaReceiver(config);
        receiver.run();
    }
}
