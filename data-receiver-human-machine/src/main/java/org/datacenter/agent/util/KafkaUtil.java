package org.datacenter.agent.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringSerializer;
import org.datacenter.exception.ZorathosException;
import org.datacenter.receiver.util.RetryUtil;

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
    private static final Integer MAX_RETRY_COUNT = Integer.parseInt(humanMachineProperties.getProperty("agent.retries.kafka", "3"));

    static {
        // 通用配置
        adminProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                humanMachineProperties.get("kafka.bootstrap.servers"));

        // 添加安全认证配置 - AdminClient
        if (Boolean.parseBoolean(humanMachineProperties.getProperty("kafka.security.enabled", "false"))) {
            // 配置安全协议
            adminProps.put("security.protocol",
                    humanMachineProperties.getProperty("kafka.security.protocol", "SASL_PLAINTEXT"));
            // 配置SASL机制
            adminProps.put(SaslConfigs.SASL_MECHANISM,
                    humanMachineProperties.getProperty("kafka.sasl.mechanism", "PLAIN"));
            // 配置JAAS
            adminProps.put(SaslConfigs.SASL_JAAS_CONFIG,
                    String.format("org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
                            humanMachineProperties.getProperty("kafka.username"),
                            humanMachineProperties.getProperty("kafka.password")));
        }

        // Producer配置
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                humanMachineProperties.get("kafka.bootstrap.servers"));
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

        // 添加安全认证配置 - Producer
        if (Boolean.parseBoolean(humanMachineProperties.getProperty("kafka.security.enabled", "false"))) {
            // 配置安全协议
            producerProps.put("security.protocol",
                    humanMachineProperties.getProperty("kafka.security.protocol", "SASL_PLAINTEXT"));
            // 配置SASL机制
            producerProps.put(SaslConfigs.SASL_MECHANISM,
                    humanMachineProperties.getProperty("kafka.sasl.mechanism", "PLAIN"));
            // 配置JAAS
            producerProps.put(SaslConfigs.SASL_JAAS_CONFIG,
                    String.format("org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
                            humanMachineProperties.getProperty("kafka.username"),
                            humanMachineProperties.getProperty("kafka.password")));
        }
    }

    public static void createTopicIfNotExists(String topic) {
        RetryUtil.executeWithRetry(() -> {
            try (AdminClient adminClient = AdminClient.create(adminProps)) {
                Set<String> existingTopics = adminClient.listTopics().names().get();
                if (!existingTopics.contains(topic)) {
                    NewTopic newTopic = new NewTopic(topic, 1, (short) 1);
                    adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
                    log.info("Kafka topic created: {}", topic);
                } else {
                    log.info("Kafka topic already exists: {}", topic);
                }
                return null;
            } catch (InterruptedException | ExecutionException e) {
                throw new ZorathosException(e, "Failed to create topic " + topic + " in Kafka.");
            }
        }, MAX_RETRY_COUNT, "Create Kafka Topic " + topic);
    }

    public static void sendMessage(String topic, String message) {
        RetryUtil.executeWithRetry(() -> {
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
                createTopicIfNotExists(topic);
                log.info("Sending message to kafka topic: {}", topic);
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, null, message);
                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        throw new ZorathosException(exception, "Failed to send message to Kafka.");
                    }
                }).get(); // 添加get()以确保消息发送完成并捕获异常
                producer.flush();
                return null;
            } catch (ExecutionException | InterruptedException e) {
                throw new ZorathosException(e, "Failed to send message to Kafka.");
            }
        }, MAX_RETRY_COUNT, "Send Kafka Message to topic " + topic);
    }
}
