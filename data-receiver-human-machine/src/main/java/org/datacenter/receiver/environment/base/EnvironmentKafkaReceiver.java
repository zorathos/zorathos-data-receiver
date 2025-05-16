package org.datacenter.receiver.environment.base;

import org.apache.flink.api.java.utils.ParameterTool;
import org.datacenter.config.receiver.environment.EnvironmentKafkaReceiverConfig;
import org.datacenter.receiver.KafkaReceiver;

import java.io.Serial;
import java.io.Serializable;

import static org.datacenter.config.keys.HumanMachineReceiverConfigKey.ENVIRONMENT_KAFKA_TOPIC;
import static org.datacenter.config.keys.HumanMachineReceiverConfigKey.IMPORT_ID;

/**
 * @description  : [一句话描述该类的功能]
 * @author       : [wangminan]
 */
public abstract class EnvironmentKafkaReceiver<T> extends KafkaReceiver<T, EnvironmentKafkaReceiverConfig> implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    protected static EnvironmentKafkaReceiverConfig parseArgs(String[] args) {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        return EnvironmentKafkaReceiverConfig.builder()
                .importId(Long.valueOf(parameterTool.getRequired(IMPORT_ID.getKeyForParamsMap())))
                .topic(parameterTool.getRequired(ENVIRONMENT_KAFKA_TOPIC.getKeyForParamsMap()))
                .build();
    }
}
