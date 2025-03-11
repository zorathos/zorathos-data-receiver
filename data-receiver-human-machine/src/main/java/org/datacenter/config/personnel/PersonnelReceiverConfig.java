package org.datacenter.config.personnel;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.datacenter.config.receiver.BaseReceiverConfig;

/**
 * @author : [wangminan]
 * @description : 人员系统接收配置
 */
@EqualsAndHashCode(callSuper = true)
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class PersonnelReceiverConfig extends BaseReceiverConfig {
    private String queryString;
}
