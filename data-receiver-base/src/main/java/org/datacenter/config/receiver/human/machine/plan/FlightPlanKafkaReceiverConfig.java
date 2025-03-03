package org.datacenter.config.receiver.human.machine.plan;

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.datacenter.config.receiver.base.KafkaBaseReceiverConfig;

/**
 * @author : [wangminan]
 * @description : 飞行计划数据通过接口接收后投递到Kafka，Flink通过Kafka接收
 */
@EqualsAndHashCode(callSuper = true)
@Builder
@Data
public class FlightPlanKafkaReceiverConfig extends KafkaBaseReceiverConfig {
}
