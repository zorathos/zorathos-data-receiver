package org.datacenter.receiver.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;
import java.time.Duration;
import java.util.List;

import static org.datacenter.config.system.BaseSysConfig.humanMachineProperties;

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
                Duration.ofSeconds(Long.parseLong(humanMachineProperties.getProperty("flink.checkpoint.interval"))));
        configuration.set(CheckpointingOptions.CHECKPOINTING_CONSISTENCY_MODE,
                CheckpointingMode.EXACTLY_ONCE);
        configuration.set(CheckpointingOptions.CHECKPOINTING_TIMEOUT,
                Duration.ofSeconds(Long.parseLong(humanMachineProperties.getProperty("flink.checkpoint.timeout"))));
        configuration.set(CheckpointingOptions.EXTERNALIZED_CHECKPOINT_RETENTION,
                ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);

        // 开启非对齐检查点
        configuration.set(CheckpointingOptions.ENABLE_UNALIGNED, true);
        configuration.set(CheckpointingOptions.ALIGNED_CHECKPOINT_TIMEOUT,
                Duration.ofSeconds(120));

        // 根据配置创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);

        // 设置并行度为核心数量 / 2 最小为1
        env.setParallelism(Math.max(Runtime.getRuntime().availableProcessors() / 2, 1));
        return env;
    }

    /**
     * 创建Flink的Kafka数据源
     * @param env 执行环境
     * @param topics 主题
     * @param clazz 泛型
     * @return 数据源
     * @param <T> 泛型
     */
    public static <T> DataStreamSource<T> getKafkaSourceDS(StreamExecutionEnvironment env, List<String> topics, Class<T> clazz) {
        // 开始从kafka获取数据
        // 数据源
        KafkaSource<T> kafkaSource = KafkaSource.<T>builder()
                .setBootstrapServers(humanMachineProperties.getProperty("kafka.bootstrap.servers"))
                .setGroupId(humanMachineProperties.getProperty("kafka.consumer.group-id"))
                .setTopics(topics)
                .setValueOnlyDeserializer(new AbstractDeserializationSchema<T>() {
                    @Override
                    public T deserialize(byte[] message) throws IOException {
                        ObjectMapper mapper = new ObjectMapper();
                        // 根据泛型T进行转换
                        return mapper.readValue(message, clazz);
                    }
                })
                .setStartingOffsets(OffsetsInitializer.latest())
                .build();
        return env
                .fromSource(kafkaSource,
                        WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(3)),
                        "kafkaSource");
    }
}
