package org.datacenter.receiver.util;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.common.config.SaslConfigs;
import org.datacenter.config.HumanMachineConfig;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Objects;

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

@Slf4j
@SuppressWarnings("deprecation")
public class DataReceiverUtil {

    public static final ObjectMapper mapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .configure(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, true)
            .configure(JsonParser.Feature.INCLUDE_SOURCE_IN_LOCATION, true)
            .configure(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS, true);

    public static StreamExecutionEnvironment prepareStreamEnv() {
        log.info("Preparing Flink execution environment.");
        Configuration config = new Configuration();
        config.set(CheckpointingOptions.CHECKPOINTING_INTERVAL,
                Duration.ofSeconds(Long.parseLong(HumanMachineConfig.getProperty(FLINK_CHECKPOINT_INTERVAL))));
        config.set(CheckpointingOptions.CHECKPOINTING_CONSISTENCY_MODE, CheckpointingMode.EXACTLY_ONCE);
        config.set(CheckpointingOptions.CHECKPOINTING_TIMEOUT,
                Duration.ofSeconds(Long.parseLong(HumanMachineConfig.getProperty(FLINK_CHECKPOINT_TIMEOUT))));
        config.set(CheckpointingOptions.EXTERNALIZED_CHECKPOINT_RETENTION, ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
        config.set(CheckpointingOptions.ENABLE_UNALIGNED, true);
        config.set(CheckpointingOptions.ALIGNED_CHECKPOINT_TIMEOUT, Duration.ofSeconds(120));
        return StreamExecutionEnvironment.getExecutionEnvironment(config);
    }

    /**
     * 以JSON形式接收Kafka数据
     * @param env Flink的执行环境
     * @param topics Kafka主题列表
     * @param clazz Kafka消息的反序列化类
     * @return Kafka数据流
     * @param <T> Kafka消息的类型
     */
    public static <T> DataStreamSource<T> getKafkaSourceDS(StreamExecutionEnvironment env, List<String> topics, Class<T> clazz) {
        KafkaSourceBuilder<T> builder = KafkaSource.<T>builder()
                .setBootstrapServers(HumanMachineConfig.getProperty(KAFKA_BOOTSTRAP_SERVERS))
                .setGroupId(HumanMachineConfig.getProperty(KAFKA_CONSUMER_GROUP_ID))
                .setTopics(topics)
                .setValueOnlyDeserializer(new JsonDeserializer<>(clazz))
                .setStartingOffsets(OffsetsInitializer
                        .timestamp(System.currentTimeMillis() - Long.parseLong(HumanMachineConfig.getProperty(KAFKA_OFFSET_TIMESTAMP))));
        configureSasl(builder);
        return env.fromSource(builder.build(), WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(3)), "kafkaSource");
    }

    /**
     * 以JSON数组形式接收Kafka数据
     * @param env Flink的执行环境
     * @param topics Kafka主题列表
     * @param clazz Kafka消息的反序列化类
     * @return Kafka数据流
     * @param <T> Kafka消息的类型
     */
    public static <T> SingleOutputStreamOperator<T> getKafkaArraySourceDS(StreamExecutionEnvironment env, List<String> topics, Class<T> clazz) {
        KafkaSourceBuilder<List<T>> builder = KafkaSource.<List<T>>builder()
                .setBootstrapServers(HumanMachineConfig.getProperty(KAFKA_BOOTSTRAP_SERVERS))
                .setGroupId(HumanMachineConfig.getProperty(KAFKA_CONSUMER_GROUP_ID))
                .setTopics(topics)
                .setValueOnlyDeserializer(new JsonArrayDeserializer<>(clazz))
                .setStartingOffsets(OffsetsInitializer
                        .timestamp(System.currentTimeMillis() - Long.parseLong(HumanMachineConfig.getProperty(KAFKA_OFFSET_TIMESTAMP))));
        configureSasl(builder);
        return env.fromSource(builder.build(), WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(3)), "kafkaArraySource")
                .flatMap((FlatMapFunction<List<T>, T>) (list, out) -> list.stream()
                        .filter(Objects::nonNull)
                        .forEach(out::collect))
                .returns(clazz);
    }

    private static void configureSasl(KafkaSourceBuilder<?> builder) {
        if (Boolean.parseBoolean(HumanMachineConfig.getProperty(KAFKA_SECURITY_ENABLED, "false"))) {
            builder.setProperty("security.protocol", HumanMachineConfig.getProperty(KAFKA_SECURITY_PROTOCOL, "SASL_PLAINTEXT"));
            builder.setProperty(SaslConfigs.SASL_MECHANISM, HumanMachineConfig.getProperty(KAFKA_SASL_MECHANISM, "PLAIN"));
            builder.setProperty(SaslConfigs.SASL_JAAS_CONFIG, String.format(
                    "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
                    HumanMachineConfig.getProperty(KAFKA_USERNAME), HumanMachineConfig.getProperty(KAFKA_PASSWORD)));
        }
    }

    private static class JsonDeserializer<T> extends AbstractDeserializationSchema<T> {
        private final Class<T> clazz;

        public JsonDeserializer(Class<T> clazz) {
            this.clazz = clazz;
        }

        @Override
        public T deserialize(byte[] message) throws IOException {
            log.info("Receiving message from Kafka.");
            return mapper.readValue(message, clazz);
        }
    }

    private static class JsonArrayDeserializer<T> extends AbstractDeserializationSchema<List<T>> {
        private final Class<T> clazz;

        public JsonArrayDeserializer(Class<T> clazz) {
            this.clazz = clazz;
        }

        @Override
        public List<T> deserialize(byte[] message) throws IOException {
            log.info("Receiving array message from Kafka.");
            JavaType listType = mapper.getTypeFactory().constructCollectionType(List.class, clazz);
            return mapper.readValue(message, listType);
        }
    }
}
