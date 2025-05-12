package org.datacenter.agent.util;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.support.ConnectionPoolSupport;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.datacenter.config.HumanMachineConfig;
import org.datacenter.config.keys.HumanMachineSysConfigKey;
import org.datacenter.exception.ZorathosException;
import org.datacenter.receiver.util.RetryUtil;

import java.time.Duration;

/**
 * @author claude (thanks)
 * 基于lettuce的redis工具类，使用连接池优化
 */
@Slf4j
public class RedisUtil {
    private static final String REDIS_HOST = HumanMachineConfig.getProperty(HumanMachineSysConfigKey.REDIS_HOST);
    private static final Integer REDIS_PORT = Integer.parseInt(HumanMachineConfig.getProperty(HumanMachineSysConfigKey.REDIS_PORT));
    private static final CharSequence REDIS_PASSWORD = HumanMachineConfig.getProperty(HumanMachineSysConfigKey.REDIS_PASSWORD);
    private static final Integer REDIS_TIMEOUT = Integer.parseInt(HumanMachineConfig.getProperty(HumanMachineSysConfigKey.REDIS_TIMEOUT));
    private static final Integer REDIS_POOL_MAX_TOTAL =
            Integer.parseInt(HumanMachineConfig.getProperty(HumanMachineSysConfigKey.REDIS_POOL_MAX_TOTAL));
    private static final Integer REDIS_POOL_MAX_IDLE =
            Integer.parseInt(HumanMachineConfig.getProperty(HumanMachineSysConfigKey.REDIS_POOL_MAX_IDLE));
    private static final Integer REDIS_POOL_MIN_IDLE =
            Integer.parseInt(HumanMachineConfig.getProperty(HumanMachineSysConfigKey.REDIS_POOL_MIN_IDLE));
    private static final Integer MAX_RETRY_COUNT = Integer.parseInt(HumanMachineConfig.getProperty(HumanMachineSysConfigKey.AGENT_RETRIES_REDIS));

    private static volatile RedisClient redisClient;
    private static volatile GenericObjectPool<StatefulRedisConnection<String, String>> connectionPool;

    // 私有构造方法，防止实例化
    private RedisUtil() {
    }

    /**
     * 初始化Redis连接池
     */
    public static void initPool() {
        if (connectionPool == null) {
            synchronized (RedisUtil.class) {
                if (connectionPool == null) {
                    try {
                        RedisURI redisUri = RedisURI.builder()
                                .withHost(REDIS_HOST)
                                .withPort(REDIS_PORT)
                                .withPassword(REDIS_PASSWORD)
                                .withTimeout(Duration.ofSeconds(REDIS_TIMEOUT))
                                .build();

                        redisClient = RedisClient.create(redisUri);

                        // 配置连接池
                        GenericObjectPoolConfig<StatefulRedisConnection<String, String>> poolConfig =
                                new GenericObjectPoolConfig<>();
                        poolConfig.setMaxTotal(REDIS_POOL_MAX_TOTAL);
                        poolConfig.setMaxIdle(REDIS_POOL_MAX_IDLE);
                        poolConfig.setMinIdle(REDIS_POOL_MIN_IDLE);
                        poolConfig.setTestOnBorrow(true);
                        poolConfig.setTestOnReturn(true);
                        poolConfig.setTestWhileIdle(true);

                        // 创建连接池
                        connectionPool = ConnectionPoolSupport.createGenericObjectPool(
                                () -> redisClient.connect(), poolConfig);

                        log.info("Redis connection pool initialized successfully");
                    } catch (Exception e) {
                        throw new ZorathosException(e, "Failed to initialize Redis connection pool");
                    }
                }
            }
        }
    }

    /**
     * 获取Redis值（带重试机制）
     */
    public static String get(String key) {
        return RetryUtil.executeWithRetry(() -> {
            try (StatefulRedisConnection<String, String> connection = connectionPool.borrowObject()) {
                RedisCommands<String, String> commands = connection.sync();
                return commands.get(key);
            } catch (Exception e) {
                log.error("Failed to fetch the key: {}", key, e);
                throw new ZorathosException(e, "Failed to fetch the key: " + key);
            }
        }, MAX_RETRY_COUNT, "Redis GET " + key);
    }

    /**
     * 设置Redis键值（带重试机制）
     */
    public static Boolean set(String key, String value) {
        RetryUtil.executeWithRetry(() -> {
            try (StatefulRedisConnection<String, String> connection = connectionPool.borrowObject()) {
                RedisCommands<String, String> commands = connection.sync();
                commands.set(key, value);
                log.info("Redis key: {} set successfully", key);
                return true;
            } catch (Exception e) {
                log.error("Failed to set the key: {}", key, e);
                throw new ZorathosException(e, "Failed to set the key: " + key);
            }
        }, MAX_RETRY_COUNT, "Redis SET " + key);
        return false;
    }

    /**
     * 设置Redis键值（仅当键不存在时）（带重试机制）
     */
    public static Boolean setnx(String key, String value) {
        return RetryUtil.executeWithRetry(() -> {
            try (StatefulRedisConnection<String, String> connection = connectionPool.borrowObject()) {
                RedisCommands<String, String> commands = connection.sync();
                return commands.setnx(key, value);
            } catch (Exception e) {
                log.error("SETNX for key failed, key: {}", key, e);
                throw new ZorathosException(e, "Redis SETNX failed, key: " + key);
            }
        }, MAX_RETRY_COUNT, "Redis SETNX " + key);
    }

    /**
     * 设置键过期时间（秒）（带重试机制）
     */
    public static Boolean expire(String key, long seconds) {
        RetryUtil.executeWithRetry(() -> {
            try (StatefulRedisConnection<String, String> connection = connectionPool.borrowObject()) {
                RedisCommands<String, String> commands = connection.sync();
                commands.expire(key, seconds);
                log.info("Redis key: {} set expire time successfully", key);
                return true;
            } catch (Exception e) {
                log.error("Failed to set expire time for redis key: {}", key, e);
                throw new ZorathosException(e, "Failed to set expire time for redis key: " + key);
            }
        }, MAX_RETRY_COUNT, "Redis EXPIRE " + key);
        return false;
    }

    /**
     * 删除键（带重试机制）
     */
    public static Boolean del(String key) {
        return RetryUtil.executeWithRetry(() -> {
            try (StatefulRedisConnection<String, String> connection = connectionPool.borrowObject()) {
                RedisCommands<String, String> commands = connection.sync();
                Long result = commands.del(key);
                log.info("Redis key: {} deleted successfully, result={}", key, result);
                return result > 0;
            } catch (Exception e) {
                log.error("Failed to del the key: {}", key, e);
                throw new ZorathosException(e);
            }
        }, MAX_RETRY_COUNT, "Redis DEL " + key);
    }

    /**
     * 检查键是否存在（带重试机制）
     */
    public static Boolean exists(String key) {
        return RetryUtil.executeWithRetry(() -> {
            try (StatefulRedisConnection<String, String> connection = connectionPool.borrowObject()) {
                RedisCommands<String, String> commands = connection.sync();
                return commands.exists(key) > 0;
            } catch (Exception e) {
                log.error("Failed to check if redis key exists, key: {}", key, e);
                throw new ZorathosException(e, "Failed to check if redis key exists, key: " + key);
            }
        }, MAX_RETRY_COUNT, "Redis EXISTS " + key);
    }

    /**
     * 关闭Redis连接池和客户端
     */
    public static void shutdownPool() {
        try {
            if (connectionPool != null) {
                connectionPool.close();
            }
            if (redisClient != null) {
                redisClient.shutdown();
            }
            log.info("Redis connection pool has been closed successfully");
        } catch (Exception e) {
            log.error("Failed to shutdownPool redis connection pool", e);
        }
    }
}
