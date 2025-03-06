package org.datacenter.config.sinker.human.machine.personnel;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.datacenter.config.sinker.base.TiDBSinkerConfig;

/**
 * @author : [wangminan]
 * @description :
 */
@Data
@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class PersonnelSinkerConfig extends TiDBSinkerConfig {
    // 用于构建insert语句的其他变量
    private String tableName;
    private String[] columnNames;
}
