package org.datacenter.receiver.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.common.config.SaslConfigs;
import org.datacenter.config.system.HumanMachineSysConfig;

import java.io.IOException;
import java.time.Duration;
import java.util.List;

/**
 * @author : [wangminan]
 * @description : 数据接收器工具类
 */
@Slf4j
public class DataReceiverUtil {

    public static StreamExecutionEnvironment prepareStreamEnv() {
        log.info("Preparing flink execution environment.");
        // 创建配置
        Configuration configuration = new Configuration();

        configuration.set(CheckpointingOptions.CHECKPOINTING_INTERVAL,
                Duration.ofSeconds(Long.parseLong(HumanMachineSysConfig.getHumanMachineProperties().getProperty("flink.checkpoint.interval"))));
        configuration.set(CheckpointingOptions.CHECKPOINTING_CONSISTENCY_MODE,
                CheckpointingMode.EXACTLY_ONCE);
        configuration.set(CheckpointingOptions.CHECKPOINTING_TIMEOUT,
                Duration.ofSeconds(Long.parseLong(HumanMachineSysConfig.getHumanMachineProperties().getProperty("flink.checkpoint.timeout"))));
        configuration.set(CheckpointingOptions.EXTERNALIZED_CHECKPOINT_RETENTION,
                ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);

        // 开启非对齐检查点
        configuration.set(CheckpointingOptions.ENABLE_UNALIGNED, true);
        configuration.set(CheckpointingOptions.ALIGNED_CHECKPOINT_TIMEOUT,
                Duration.ofSeconds(120));

        // 根据配置创建环境

        return StreamExecutionEnvironment.getExecutionEnvironment(configuration);
    }

    /**
     * 创建Flink的Kafka数据源
     *
     * @param env    执行环境
     * @param topics 主题
     * @param clazz  泛型
     * @param <T>    泛型
     * @return 数据源
     */
    public static <T> DataStreamSource<T> getKafkaSourceDS(StreamExecutionEnvironment env, List<String> topics, Class<T> clazz) {
        // 开始从kafka获取数据
        // 数据源
        KafkaSourceBuilder<T> tKafkaSourceBuilder = KafkaSource.<T>builder()
                .setBootstrapServers(HumanMachineSysConfig.getHumanMachineProperties().getProperty("kafka.bootstrap.servers"))
                .setGroupId(HumanMachineSysConfig.getHumanMachineProperties().getProperty("kafka.consumer.group-id"))
                .setTopics(topics)
                .setValueOnlyDeserializer(new AbstractDeserializationSchema<T>(clazz) {
                    @Override
                    public T deserialize(byte[] message) throws IOException {
                        log.info("Receiving message from kafka with topics:{}.", topics);
                        ObjectMapper mapper = new ObjectMapper();
                        mapper.registerModule(new JavaTimeModule());
                        return mapper.readValue(message, clazz);
                    }
                })
                // 这个位置有待商榷 确实会因为latest导致agent已经发了但是这边拿不到
                .setStartingOffsets(OffsetsInitializer.timestamp(System.currentTimeMillis() -
                        Long.parseLong(HumanMachineSysConfig.getHumanMachineProperties().getProperty("kafka.offset.timestamp"))));
        // 判断是否需要开启SASL认证
        if (Boolean.parseBoolean(HumanMachineSysConfig.getHumanMachineProperties().getProperty("kafka.security.enabled", "false"))) {
            tKafkaSourceBuilder.setProperty("security.protocol",
                    HumanMachineSysConfig.getHumanMachineProperties().getProperty("kafka.security.protocol", "SASL_PLAINTEXT"));
            tKafkaSourceBuilder.setProperty(SaslConfigs.SASL_MECHANISM,
                    HumanMachineSysConfig.getHumanMachineProperties().getProperty("kafka.sasl.mechanism", "PLAIN"));
            tKafkaSourceBuilder.setProperty(SaslConfigs.SASL_JAAS_CONFIG,
                    String.format("org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
                            HumanMachineSysConfig.getHumanMachineProperties().getProperty("kafka.username"),
                            HumanMachineSysConfig.getHumanMachineProperties().getProperty("kafka.password")));
        }
        KafkaSource<T> kafkaSource = tKafkaSourceBuilder.build();
        return env
                .fromSource(kafkaSource,
                        WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(3)),
                        "kafkaSource");
    }
}
