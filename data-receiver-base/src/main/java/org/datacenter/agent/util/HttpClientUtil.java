package org.datacenter.agent.util;

import java.util.ArrayList;
import java.util.List;

/**
 * @author : [wangminan]
 * @description : 在本类中，我们调用HttpClient发送请求
 */
public class HttpClientUtil {
    // 本地缓存 是人员与装备系统的cookie列表
    public static List<String> personnelAndEquipmentSystemCookies;

    static {
        // 初始化人员与装备系统的cookie列表
        personnelAndEquipmentSystemCookies = new ArrayList<>();
    }

    /**
     * 登录人员与装备系统
     */
    public void loginToPersonnelAndEquipmentSystem() {

    }
}
