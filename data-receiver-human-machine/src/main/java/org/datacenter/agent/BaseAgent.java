package org.datacenter.agent;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.datacenter.agent.util.MinioUtil;
import org.datacenter.exception.ZorathosException;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.datacenter.config.system.BaseSysConfig.humanMachineProperties;

/**
 * Agent只出现在人际能力匹配验证系统中 用于从老项目接口中拉取数据并投递
 * Agent理论上只能启动一个单一实例，所以我们通过给定路径下是否存在文件来判断
 * 如果无法都调度到本地可能要通过s3扩展来接入
 *
 * @author : [wangminan]
 * @description : 基础的Agent，如有需要 将在Receiver的Prepare阶段被调用
 */
@Slf4j
@Data
public abstract class BaseAgent implements Runnable {

    /**
     * Agent是否已经启动 由文件初始化
     */
    protected volatile boolean running = false;

    /**
     * 由本实例启动
     */
    protected boolean isStartedByThisInstance = false;

    protected String basePath = humanMachineProperties.getProperty("agent.running.basePath");

    public BaseAgent() {
        boolean fileExist = MinioUtil.checkFileExist(basePath + '/' + this.getClass().getSimpleName());
        if (fileExist) {
            running = true;
        }
    }

    @Override
    public void run() {
        if (running) {
            log.info("Agent is running");
            return;
        }
        try {
            Path tempFile = Files.createTempFile(this.getClass().getSimpleName(), "");
            // 随便写点什么进去
            Files.writeString(tempFile, "running");
            log.info("Temp file created: {}", tempFile);
            // tempFile转inputStream
            InputStream inputStream = Files.newInputStream(tempFile);
            MinioUtil.upload(basePath + '/' + this.getClass().getSimpleName(), inputStream);
            running = true;
            isStartedByThisInstance = true;
        } catch (IOException e) {
            throw new ZorathosException(e, "Encountered an error while creating temp file");
        }
    }

    public void stop() {
        running = false;
        if (isStartedByThisInstance) {
            // 删除文件
            MinioUtil.delete(basePath + '/' + this.getClass().getSimpleName());
        }
    }
}
