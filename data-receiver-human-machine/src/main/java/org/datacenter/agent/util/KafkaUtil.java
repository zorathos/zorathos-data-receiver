package org.datacenter.agent.util;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.datacenter.exception.ZorathosException;

import java.util.Properties;
import java.util.concurrent.Future;

import static org.datacenter.config.system.BaseSysConfig.humanMachineProperties;

/**
 * @author : [wangminan]
 * @description : 全局的Kafka工具
 */
public class KafkaUtil {
    private static final Properties producerProps = new Properties();
    private static final Properties consumerProps = new Properties();

    static {
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                humanMachineProperties.get("kafka.bootstrap.servers"));
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

        consumerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                humanMachineProperties.get("kafka.bootstrap.servers"));
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG,
                humanMachineProperties.get("kafka.consumer.group-id"));
        consumerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        consumerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                OffsetResetStrategy.LATEST.toString());

    }

    public static void sendMessage(String topic, String message) {
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, null, message);
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    throw new ZorathosException(exception, "Failed to send message to Kafka.");
                }
            });
            producer.flush();
        }
    }
}
