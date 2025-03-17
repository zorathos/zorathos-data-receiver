package org.datacenter.agent;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.datacenter.exception.ZorathosException;

import java.time.Duration;

import static org.datacenter.config.system.BaseSysConfig.humanMachineProperties;

/**
 * 基础的Agent类，使用Redis来处理启动状态，防止并发运行
 *
 * @author : [wangminan]
 * @description : 基础的Agent，如有需要将在Receiver的Prepare阶段被调用
 */
@Slf4j
@Data
public abstract class BaseAgent implements Runnable {

    protected volatile boolean prepared = false;
    protected volatile boolean running = false;
    protected boolean isStartedByThisInstance = false;

    protected String redisKey;
    protected String redisHost = humanMachineProperties.getProperty("redis.host");
    protected int redisPort = Integer.parseInt(humanMachineProperties.getProperty("redis.port"));
    protected CharSequence redisPassword = humanMachineProperties.getProperty("redis.password");
    protected RedisClient redisClient;
    protected StatefulRedisConnection<String, String> connection;
    protected RedisCommands<String, String> commands;

    public BaseAgent() {
        this.redisKey = "human-machine:agent:" + this.getClass().getSimpleName();
        try {
            RedisURI redisUri = RedisURI.builder()
                    .withHost(redisHost)
                    .withPort(redisPort)
                    .withPassword(redisPassword)
                    .withTimeout(Duration.ofSeconds(5))
                    .build();
            redisClient = RedisClient.create(redisUri);
            connection = redisClient.connect();
            commands = connection.sync();

            // 检查Redis中是否存在标识
            String status = commands.get(redisKey);
            if ("running".equals(status)) {
                prepared = true;
            }
        } catch (Exception e) {
            throw new ZorathosException(e, "Failed to connect to Redis");
        }
    }

    @Override
    public void run() {
        if (prepared) {
            log.info("Agent is already prepared.");
            return;
        }
        try {
            // 尝试设置Redis中的标识（使用SETNX实现并发锁）
            Boolean success = commands.setnx(redisKey, "running");
            if (Boolean.TRUE.equals(success)) {
                // 设置过期时间，防止意外中断后长期锁住
                commands.expire(redisKey, 60 * 60);

                log.info("Agent started and status set in Redis.");
                prepared = true;
                isStartedByThisInstance = true;
            } else {
                log.info("Agent is already running (Redis key exists).");
            }
        } catch (Exception e) {
            throw new ZorathosException(e, "Failed to set Redis key");
        }
    }

    /**
     * 必须在shutdownhook中调用，用于清理Redis中的标识
     */
    public void stop() {
        log.info("Agent is stopping.");
        prepared = false;
        running = false;

        if (isStartedByThisInstance) {
            try {
                // 删除Redis中的标识
                commands.del(redisKey);
                log.info("Agent stopped and Redis key removed.");
            } catch (Exception e) {
                log.error("Failed to delete Redis key", e);
            }
        }

        // 关闭Redis连接
        closeRedisConnection();
    }

    private void closeRedisConnection() {
        try {
            if (connection != null) {
                connection.close();
            }
            if (redisClient != null) {
                redisClient.shutdown();
            }
        } catch (Exception e) {
            log.error("Failed to close Redis connection", e);
        }
    }
}
