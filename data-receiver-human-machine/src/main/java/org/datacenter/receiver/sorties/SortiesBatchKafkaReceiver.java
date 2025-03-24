package org.datacenter.receiver.sorties;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.jdbc.sink.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.datacenter.agent.sorties.SortiesBatchAgent;
import org.datacenter.config.system.HumanMachineSysConfig;
import org.datacenter.exception.ZorathosException;
import org.datacenter.model.base.TiDBDatabase;
import org.datacenter.model.sorties.SortiesBatch;
import org.datacenter.receiver.BaseReceiver;
import org.datacenter.receiver.util.DataReceiverUtil;
import org.datacenter.receiver.util.JdbcSinkUtil;

import java.util.List;

import static org.datacenter.config.system.BaseSysConfig.humanMachineProperties;

/**
 * @author : [wangminan]
 * @description : 从Kafka中接收架次批数据写入TiDB
 */
@Slf4j
public class SortiesBatchKafkaReceiver extends BaseReceiver {

    private final SortiesBatchAgent sortiesBatchAgent;

    public SortiesBatchKafkaReceiver() {
        // 1. 加载配置 HumanMachineSysConfig.loadConfig();
        HumanMachineSysConfig sysConfig = new HumanMachineSysConfig();
        sysConfig.loadConfig();
        this.sortiesBatchAgent = new SortiesBatchAgent();
    }


    @Override
    public void prepare() {
        super.prepare();
        sortiesBatchAgent.run();
        awaitAgentRunning(sortiesBatchAgent);
    }

    @Override
    public void start() {
        //shutdownhook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                log.info("Shutting down SortiesBatchKafkaReceiver...");
                sortiesBatchAgent.stop();
            } catch (Exception e) {
                throw new ZorathosException(e, "Encounter error when stopping sorties batch agent. You may need to check minio to delete remote tmp file.");
            }
        }));

        // 开始从kafka获取数据
        // 引入执行环境
        StreamExecutionEnvironment env = DataReceiverUtil.prepareStreamEnv();
        DataStreamSource<SortiesBatch> kafkaSourceDS =
                DataReceiverUtil.getKafkaSourceDS(env, List.of(humanMachineProperties.getProperty("kafka.topic.sortiesBatch")), SortiesBatch.class);

        Sink<SortiesBatch> sinkFunction = JdbcSink.<SortiesBatch>builder()
                .withQueryStatement("""
                                INSERT INTO `sorties_batch` (
                                    `id`, `batch_number`
                                ) VALUES (
                                    ?, ?
                                ) ON DUPLICATE KEY UPDATE
                                    batch_number = VALUES(batch_number);
                                """,
                        (JdbcStatementBuilder<SortiesBatch>) (preparedStatement, sortiesBatch) -> {
                            preparedStatement.setString(1, sortiesBatch.getId());
                            preparedStatement.setString(2, sortiesBatch.getBatchNumber());
                        })
                .withExecutionOptions(JdbcSinkUtil.getTiDBJdbcExecutionOptions())
                .buildAtLeastOnce(JdbcSinkUtil.getTiDBJdbcConnectionOptions(TiDBDatabase.SORTIES));


        kafkaSourceDS.sinkTo(sinkFunction).name("SortiesBatch Kafka Sinker");
        try {
            env.execute("SortiesBatch Kafka Receiver");
        } catch (Exception e) {
            throw new ZorathosException(e, "Encounter error when executing SortiesBatchKafkaReceiver.");
        }
    }

    public static void main(String[] args) {
        SortiesBatchKafkaReceiver receiver = new SortiesBatchKafkaReceiver();
        receiver.run();
    }
}
