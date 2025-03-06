package org.datacenter.config.system;

import lombok.Data;

import java.util.Properties;

/**
 * @author : [wangminan]
 * @description : 系统配置基类
 */
@Data
public abstract class BaseSysConfig {

    protected final String ZORATHOS_HUMAN_MACHINE_CONFIG = "ZORATHOS_HUMAN_MACHINE_CONFIG";

    /**
     * 最终提供给外部使用的系统properties
     */
    public static Properties humanMachineProperties;

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
