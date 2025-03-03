package org.datacenter.config.system;

/**
 * @author : [wangminan]
 * @description : 系统配置基类
 */
public abstract class BaseSysConfig {

    /**
     * 从外部加载配置
     */
    public abstract void loadConfig();

    /**
     * 保存配置到外部
     */
    public abstract void saveConfig();
}
