package org.datacenter.config.receiver.human.machine.personnel;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.datacenter.config.receiver.base.KafkaBaseReceiverConfig;

/**
 * @author : [wangminan]
 * @description : 人员信息接收配置
 */
@EqualsAndHashCode(callSuper = true)
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class PersonnelKafkaReceiverConfig extends KafkaBaseReceiverConfig {
    private String topic;
}
