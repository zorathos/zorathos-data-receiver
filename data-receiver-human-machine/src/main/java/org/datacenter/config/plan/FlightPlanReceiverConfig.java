package org.datacenter.config.plan;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.datacenter.config.receiver.BaseReceiverConfig;

import java.util.Set;

/**
 * @author : [wangminan]
 * @description : 飞行计划配置
 */
@EqualsAndHashCode(callSuper = true)
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class FlightPlanReceiverConfig extends BaseReceiverConfig {
    private Set<String> queryCodes;
}
