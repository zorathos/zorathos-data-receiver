package org.datacenter.agent.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.datacenter.exception.ZorathosException;

import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.datacenter.config.system.BaseSysConfig.humanMachineProperties;

/**
 * @author : [wangminan]
 * @description : 全局的Kafka工具
 */
@Slf4j
public class KafkaUtil {
    private static final Properties adminProps = new Properties();
    private static final Properties producerProps = new Properties();

    static {
        adminProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                humanMachineProperties.get("kafka.bootstrap.servers"));

        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                humanMachineProperties.get("kafka.bootstrap.servers"));
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
    }

    private static void createTopicIfNotExists(String topic) {
        try (AdminClient adminClient = AdminClient.create(adminProps)) {
            Set<String> existingTopics = adminClient.listTopics().names().get();
            if (!existingTopics.contains(topic)) {
                NewTopic newTopic = new NewTopic(topic, 1, (short) 1);
                adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
                log.info("Kafka topic created: {}", topic);
            } else {
                log.info("Kafka topic already exists: {}", topic);
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new ZorathosException(e, "Failed to create topic " + topic +" in Kafka.");
        }
    }

    public static void sendMessage(String topic, String message) {
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
            createTopicIfNotExists(topic);
            log.debug("Sending message to kafka -> {}:{}", topic, message);
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
