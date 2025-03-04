package org.datacenter.config.receiver.human.machine.equipment;

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.datacenter.config.receiver.base.receiver.DatabaseJdbcBaseReceiverConfig;

/**
 * @author : [wangminan]
 * @description : 从Hive导入EquipmentInfo的有关配置
 */
@EqualsAndHashCode(callSuper = true)
@Builder
@Data
public class EquipmentInfoHiveReceiverConfig extends DatabaseJdbcBaseReceiverConfig {
}
