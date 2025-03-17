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
 * Agent理论上只能启动一个单一实例(不好意思但确实不能并发)，所以我们通过s3给定路径下是否存在文件来判断
 * 只有确实由本实例启动的agent有写入和删除文件的权利
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
            // 先级联创建文件夹
            Files.createDirectories(Path.of(basePath));
            Path tempFile = Files.createFile(Path.of(basePath + '/' + this.getClass().getSimpleName()));
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

    /**
     * stop方法必须在shutdownhook中被调用一次 如果不调用则文件无法清理
     * 同理的 如果对进程使用kill -9也会造成问题 最终落到外面就是停止flink的时候一定要优雅
     */
    public void stop() {
        log.info("Agent is stopping.");
        running = false;
        if (isStartedByThisInstance) {
            // 删除文件
            MinioUtil.delete(basePath + '/' + this.getClass().getSimpleName());
        }
    }
}
