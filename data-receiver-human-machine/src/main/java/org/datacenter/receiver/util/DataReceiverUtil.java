package org.datacenter.receiver.util;

import com.fasterxml.jackson.core.JsonParser;
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
import org.datacenter.config.HumanMachineConfig;

import java.io.IOException;
import java.time.Duration;
import java.util.List;

import static org.datacenter.config.keys.HumanMachineSysConfigKey.FLINK_CHECKPOINT_INTERVAL;
import static org.datacenter.config.keys.HumanMachineSysConfigKey.FLINK_CHECKPOINT_TIMEOUT;
import static org.datacenter.config.keys.HumanMachineSysConfigKey.KAFKA_BOOTSTRAP_SERVERS;
import static org.datacenter.config.keys.HumanMachineSysConfigKey.KAFKA_CONSUMER_GROUP_ID;
import static org.datacenter.config.keys.HumanMachineSysConfigKey.KAFKA_OFFSET_TIMESTAMP;
import static org.datacenter.config.keys.HumanMachineSysConfigKey.KAFKA_PASSWORD;
import static org.datacenter.config.keys.HumanMachineSysConfigKey.KAFKA_SASL_MECHANISM;
import static org.datacenter.config.keys.HumanMachineSysConfigKey.KAFKA_SECURITY_ENABLED;
import static org.datacenter.config.keys.HumanMachineSysConfigKey.KAFKA_SECURITY_PROTOCOL;
import static org.datacenter.config.keys.HumanMachineSysConfigKey.KAFKA_USERNAME;

/**
 * @author : [wangminan]
 * @description : 数据接收器工具类
 */
@Slf4j
@SuppressWarnings("deprecation")
public class DataReceiverUtil {

    public static ObjectMapper mapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .configure(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, true)
            .configure(JsonParser.Feature.INCLUDE_SOURCE_IN_LOCATION, true)
            .configure(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS, true);

    public static StreamExecutionEnvironment prepareStreamEnv() {
        log.info("Preparing flink execution environment.");
        // 创建配置
        Configuration configuration = new Configuration();

        configuration.set(CheckpointingOptions.CHECKPOINTING_INTERVAL,
                Duration.ofSeconds(Long.parseLong(HumanMachineConfig.getProperty(FLINK_CHECKPOINT_INTERVAL))));
        configuration.set(CheckpointingOptions.CHECKPOINTING_CONSISTENCY_MODE,
                CheckpointingMode.EXACTLY_ONCE);
        configuration.set(CheckpointingOptions.CHECKPOINTING_TIMEOUT,
                Duration.ofSeconds(Long.parseLong(HumanMachineConfig.getProperty(FLINK_CHECKPOINT_TIMEOUT))));
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
                .setBootstrapServers(HumanMachineConfig.getProperty(KAFKA_BOOTSTRAP_SERVERS))
                .setGroupId(HumanMachineConfig.getProperty(KAFKA_CONSUMER_GROUP_ID))
                .setTopics(topics)
                .setValueOnlyDeserializer(new AbstractDeserializationSchema<T>(clazz) {
                    @Override
                    public T deserialize(byte[] message) throws IOException {
                        log.info("Receiving message from kafka with topics:{}.", topics);
                        mapper.registerModule(new JavaTimeModule());
                        return mapper.readValue(message, clazz);
                    }
                })
                // 这个位置有待商榷 确实会因为latest导致agent已经发了但是这边拿不到
                .setStartingOffsets(OffsetsInitializer.timestamp(System.currentTimeMillis() -
                        Long.parseLong(HumanMachineConfig.getProperty(KAFKA_OFFSET_TIMESTAMP))));
        // 判断是否需要开启SASL认证
        if (Boolean.parseBoolean(HumanMachineConfig.getProperty(KAFKA_SECURITY_ENABLED, "false"))) {
            tKafkaSourceBuilder.setProperty("security.protocol",
                    HumanMachineConfig.getProperty(KAFKA_SECURITY_PROTOCOL, "SASL_PLAINTEXT"));
            tKafkaSourceBuilder.setProperty(SaslConfigs.SASL_MECHANISM,
                    HumanMachineConfig.getProperty(KAFKA_SASL_MECHANISM, "PLAIN"));
            tKafkaSourceBuilder.setProperty(SaslConfigs.SASL_JAAS_CONFIG,
                    String.format("org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
                            HumanMachineConfig.getProperty(KAFKA_USERNAME),
                            HumanMachineConfig.getProperty(KAFKA_PASSWORD)));
        }
        KafkaSource<T> kafkaSource = tKafkaSourceBuilder.build();
        return env
                .fromSource(kafkaSource,
                        WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(3)),
                        "kafkaSource");
    }
}
