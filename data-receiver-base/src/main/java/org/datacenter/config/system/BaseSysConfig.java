package org.datacenter.config.system;

import lombok.Data;

import java.util.Properties;

/**
 * @author : [wangminan]
 * @description : 系统配置基类
 */
@Data
public abstract class BaseSysConfig {

    protected static final String ZORATHOS_HUMAN_MACHINE_CONFIG = "ZORATHOS_HUMAN_MACHINE_CONFIG";

    public static Properties humanMachineProperties;

    /**
     * 从外部加载配置
     */
    public static void loadConfig() {

    };

    /**
     * 保存配置到外部
     */
    public static void saveConfig() {

    };
}
