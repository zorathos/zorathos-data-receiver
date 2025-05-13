package org.datacenter.receiver.sorties;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.jdbc.sink.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.datacenter.agent.sorties.SortiesBatchAgent;
import org.datacenter.config.HumanMachineConfig;
import org.datacenter.config.receiver.sorties.SortiesBatchReceiverConfig;
import org.datacenter.exception.ZorathosException;
import org.datacenter.model.base.TiDBDatabase;
import org.datacenter.model.sorties.SortiesBatch;
import org.datacenter.receiver.BaseReceiver;
import org.datacenter.receiver.sorties.function.SortiesBatchProcessFunction;
import org.datacenter.receiver.util.DataReceiverUtil;
import org.datacenter.receiver.util.JdbcSinkUtil;

import java.io.Serial;
import java.io.Serializable;
import java.util.Base64;
import java.util.List;

import static org.datacenter.config.keys.HumanMachineReceiverConfigKey.IMPORT_ID;
import static org.datacenter.config.keys.HumanMachineReceiverConfigKey.SORTIES_BATCH_JSON;
import static org.datacenter.config.keys.HumanMachineReceiverConfigKey.SORTIES_BATCH_URL;
import static org.datacenter.config.keys.HumanMachineReceiverConfigKey.SORTIES_RUN_MODE;
import static org.datacenter.config.keys.HumanMachineSysConfigKey.KAFKA_TOPIC_SORTIES_BATCH;

/**
 * @author : [wangminan]
 * @description : 从Kafka中接收架次批数据写入TiDB
 */
@Slf4j
public class SortiesBatchKafkaReceiver extends BaseReceiver implements Serializable {
    @Serial
    private static final long serialVersionUID = 12213146L;

    private final SortiesBatchAgent sortiesBatchAgent;
    private final SortiesBatchReceiverConfig receiverConfig;
    private static final Long STOP_SIGNAL_DURATION = 30000L;

    public SortiesBatchKafkaReceiver(SortiesBatchReceiverConfig receiverConfig) {
        // 1. 加载配置 HumanMachineConfig.loadConfig();
        HumanMachineConfig sysConfig = new HumanMachineConfig();
        sysConfig.loadConfig();
        this.sortiesBatchAgent = new SortiesBatchAgent(receiverConfig);
        this.receiverConfig = receiverConfig;
    }

    /**
     * 创建静态的JdbcSink，避免捕获外部实例导致序列化问题
     *
     * @param importId 导入ID
     * @return JdbcSink实例
     */
    private static Sink<SortiesBatch> createSortiesBatchSink(String importId) {
        return JdbcSink.<SortiesBatch>builder()
                .withQueryStatement("""
                                INSERT INTO `sorties_batch` (
                                    `id`, `batch_number`, `import_id`
                                ) VALUES (
                                    ?, ?, ?
                                ) ON DUPLICATE KEY UPDATE
                                    batch_number = VALUES(batch_number),
                                    import_id = VALUES(import_id);
                                """,
                        (preparedStatement, sortiesBatch) -> {
                            preparedStatement.setString(1, sortiesBatch.getId());
                            preparedStatement.setString(2, sortiesBatch.getBatchNumber());
                            preparedStatement.setString(3, importId);
                        })
                .withExecutionOptions(JdbcSinkUtil.getTiDBJdbcExecutionOptions())
                .buildAtLeastOnce(JdbcSinkUtil.getTiDBJdbcConnectionOptions(TiDBDatabase.SORTIES));
    }

    @Override
    public void prepare() {
        super.prepare();
        Thread agentThread = new Thread(sortiesBatchAgent);
        agentThread.setUncaughtExceptionHandler((thread, throwable) -> {
            log.error("Sorties batch agent thread {} encountered an error: {}", thread.getName(), throwable.getMessage());
            agentShutdown(sortiesBatchAgent);
        });
        agentThread.start();
        awaitAgentRunning(sortiesBatchAgent);
    }

    @Override
    public void start() {
        //shutdownhook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> agentShutdown(sortiesBatchAgent)));

        // 开始从kafka获取数据
        // 引入执行环境
        StreamExecutionEnvironment env = DataReceiverUtil.prepareStreamEnv();
        env.setParallelism(1);
        DataStreamSource<SortiesBatch> kafkaSourceDS =
                DataReceiverUtil.getKafkaSourceDS(env, List.of(HumanMachineConfig.getProperty(KAFKA_TOPIC_SORTIES_BATCH)), SortiesBatch.class);

        // 使用静态方法创建sink
        Sink<SortiesBatch> sinkFunction = createSortiesBatchSink(receiverConfig.getImportId());

        kafkaSourceDS
                .keyBy(SortiesBatch::getBatchNumber)
                .process(new SortiesBatchProcessFunction(receiverConfig.getRunMode(), STOP_SIGNAL_DURATION))
                .sinkTo(sinkFunction).name("SortiesBatch Kafka Sinker");
        try {
            env.execute("SortiesBatch Kafka Receiver");
        } catch (Exception e) {
            throw new ZorathosException(e, "Encounter error when executing SortiesBatchKafkaReceiver.");
        }
    }

    /**
     * 主函数
     *
     * @param args 入参 --url xxx --json xxx
     */
    public static void main(String[] args) {
        System.setProperty("sun.io.serialization.extendedDebugInfo", "true");
        ParameterTool params = ParameterTool.fromArgs(args);
        log.info("Params: {}", params.toMap());
        String encodedJson = params.getRequired(SORTIES_BATCH_JSON.getKeyForParamsMap());
        String decodedJson = new String(Base64.getDecoder().decode(encodedJson));

        SortiesBatchReceiverConfig receiverConfig = SortiesBatchReceiverConfig.builder()
                .importId(params.getRequired(IMPORT_ID.getKeyForParamsMap()))
                .url(params.getRequired(SORTIES_BATCH_URL.getKeyForParamsMap()))
                .runMode(SortiesBatchReceiverConfig.RunMode.fromString(
                        params.getRequired(SORTIES_RUN_MODE.getKeyForParamsMap())))
                .json(decodedJson)
                .build();
        SortiesBatchKafkaReceiver receiver = new SortiesBatchKafkaReceiver(receiverConfig);
        receiver.run();
    }
}
