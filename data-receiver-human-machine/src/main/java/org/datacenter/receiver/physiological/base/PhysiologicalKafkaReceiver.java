package org.datacenter.receiver.physiological.base;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.datacenter.config.receiver.physiological.PhysiologicalKafkaReceiverConfig;
import org.datacenter.receiver.KafkaReceiver;

import static org.datacenter.config.keys.HumanMachineReceiverConfigKey.IMPORT_ID;
import static org.datacenter.config.keys.HumanMachineReceiverConfigKey.PHYSIOLOGY_KAFKA_TOPIC;

/**
 * @author : [wangminan]
 * @description : 生理数据Kafka接收器基类
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Slf4j
public abstract class PhysiologicalKafkaReceiver<T> extends KafkaReceiver<T, PhysiologicalKafkaReceiverConfig> {

    /**
     * 解析命令行参数
     */
    protected static PhysiologicalKafkaReceiverConfig parseArgs(String[] args) {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        return PhysiologicalKafkaReceiverConfig.builder()
                .importId(Long.valueOf(parameterTool.getRequired(IMPORT_ID.getKeyForParamsMap())))
                .topic(parameterTool.getRequired(PHYSIOLOGY_KAFKA_TOPIC.getKeyForParamsMap()))
                .build();
    }
}
