package org.datacenter.config.system;

import lombok.Data;

/**
 * @author : [wangminan]
 * @description : 系统配置基类
 */
@Data
public abstract class BaseSysConfig {

    /**
     * 从外部加载配置
     */
    public void loadConfig() {
    };

    /**
     * 保存配置到外部
     */
    public void saveConfig() {
    };
}
