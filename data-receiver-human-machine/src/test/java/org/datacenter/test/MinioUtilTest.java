package org.datacenter.test;

import org.datacenter.config.HumanMachineConfig;
import org.datacenter.config.util.MinioUtil;
import org.junit.jupiter.api.Test;

/**
 * @author : [wangminan]
 * @description : 测试MinIOUtil
 */
public class MinioUtilTest {

    @Test
    void testDownload() {
        HumanMachineConfig config = new HumanMachineConfig();
        config.loadConfig();

        MinioUtil.download("simulation/simulated_data_large.csv", "D:\\data.csv");
    }
}
