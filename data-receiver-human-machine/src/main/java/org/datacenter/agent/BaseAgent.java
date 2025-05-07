package org.datacenter.agent;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.datacenter.agent.util.RedisUtil;
import org.datacenter.exception.ZorathosException;

/**
 * @author wangminan
 * 基础的Agent类，使用Redis来处理启动状态，防止并发运行
 */
@Slf4j
@Data
public abstract class BaseAgent implements Runnable {

    protected volatile boolean prepared = false;
    protected volatile boolean running = false;
    protected boolean isStartedByThisInstance = false;

    protected String redisKey;

    public BaseAgent() {
        RedisUtil.initPool();
        this.redisKey = "human-machine:agent:" + this.getClass().getSimpleName();
        try {
            // 检查Redis中是否存在标识
            String status = RedisUtil.get(redisKey);
            if ("running".equals(status)) {
                prepared = true;
            }
        } catch (Exception e) {
            throw new ZorathosException(e, "Failed to check agent status");
        }
    }

    @Override
    public void run() {
        if (prepared) {
            log.info("Another instance of agent has been started, already prepared.");
            return;
        }
        try {
            log.info("Setting key in Redis to indicate agent is running...");
            // 使用SETNX实现并发锁
            Boolean success = RedisUtil.setnx(redisKey, "running");
            if (Boolean.TRUE.equals(success)) {
                // 设置过期时间，防止意外中断后长期锁住
                RedisUtil.expire(redisKey, 60 * 60);

                log.info("Agent is starting...");
                prepared = true;
                isStartedByThisInstance = true;
            } else {
                log.info("Agent is already running in another instance (Redis key exists).");
            }
        } catch (Exception e) {
            throw new ZorathosException(e, "Failed to start the agent");
        }
    }

    /**
     * 必须在shutdownhook中调用，用于清理Redis中的标识
     * 这是外部线程正常停止的逻辑 不是异常停止
     */
    public void stop() {
        if (!prepared && !running) {
            log.info("Agent is not running, no need to stop.");
            return;
        }
        log.info("Agent is stopping...");
        prepared = false;
        running = false;

        if (isStartedByThisInstance) {
            try {
                log.info("Started by this instance, removing Redis key...");
                // 删除Redis中的标识
                RedisUtil.del(redisKey);
                log.info("Agent stopped, Redis key removed.");
            } catch (Exception e) {
                log.error("Failed to delete Redis key", e);
            }
        }

        RedisUtil.shutdownPool();
    }
}
