package org.datacenter.receiver.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
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
public class DataReceiverUtil {

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
        // 精确一次要求开启checkpoint
        env.enableCheckpointing(
                Long.parseLong(humanMachineProperties.getProperty("flink.kafka.checkpoint.interval")),
                CheckpointingMode.EXACTLY_ONCE);
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
