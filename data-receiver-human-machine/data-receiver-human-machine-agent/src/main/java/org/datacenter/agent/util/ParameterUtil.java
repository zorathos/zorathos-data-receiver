package org.datacenter.agent.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * 简易的参数解析工具，类似于Flink的ParameterTool
 * 支持从命令行参数、属性文件或Map中读取配置
 */
public class ParameterUtil {

    private final Map<String, String> data;

    /**
     * 创建一个空的ParameterUtil
     */
    public ParameterUtil() {
        this.data = new HashMap<>();
    }

    /**
     * 创建一个包含给定参数的ParameterUtil
     *
     * @param data 初始参数集合
     */
    private ParameterUtil(Map<String, String> data) {
        this.data = new HashMap<>(data);
    }

    public Map<String, String> toMap() {
        return new HashMap<>(data);
    }

    /**
     * 从命令行参数解析，格式为 --key value 或 -key value
     *
     * @param args 命令行参数数组
     * @return ParameterUtil实例
     */
    public static ParameterUtil fromArgs(String[] args) {
        Map<String, String> map = new HashMap<>();

        int i = 0;
        while (i < args.length) {
            String arg = args[i];

            if (arg.startsWith("--") || arg.startsWith("-")) {
                String key = arg.replaceFirst("^-+", "");

                if (i + 1 < args.length && !args[i + 1].startsWith("-")) {
                    map.put(key, args[i + 1]);
                    i += 2;
                } else {
                    map.put(key, "true"); // 标志参数
                    i += 1;
                }
            } else {
                i += 1;
            }
        }

        return new ParameterUtil(map);
    }

    /**
     * 从属性文件加载参数
     *
     * @param path 属性文件路径
     * @return ParameterUtil实例
     * @throws IOException 如果文件读取失败
     */
    public static ParameterUtil fromPropertiesFile(String path) throws IOException {
        try (InputStream inputStream = new FileInputStream(path)) {
            return fromPropertiesFile(inputStream);
        }
    }

    /**
     * 从属性文件输入流加载参数
     *
     * @param inputStream 属性文件输入流
     * @return ParameterUtil实例
     * @throws IOException 如果文件读取失败
     */
    public static ParameterUtil fromPropertiesFile(InputStream inputStream) throws IOException {
        Properties properties = new Properties();
        properties.load(inputStream);

        Map<String, String> map = new HashMap<>();
        for (String key : properties.stringPropertyNames()) {
            map.put(key, properties.getProperty(key));
        }

        return new ParameterUtil(map);
    }

    /**
     * 从Map创建参数工具
     *
     * @param map 包含参数的Map
     * @return ParameterUtil实例
     */
    public static ParameterUtil fromMap(Map<String, String> map) {
        return new ParameterUtil(map);
    }

    /**
     * 获取字符串参数值
     *
     * @param key 键
     * @return 参数值，如果不存在返回null
     */
    public String get(String key) {
        return data.get(key);
    }

    /**
     * 获取字符串参数值，不存在则返回默认值
     *
     * @param key          键
     * @param defaultValue 默认值
     * @return 参数值，如果不存在返回默认值
     */
    public String get(String key, String defaultValue) {
        return data.getOrDefault(key, defaultValue);
    }

    /**
     * 获取必需的参数值，如果不存在则抛出异常
     *
     * @param key 键
     * @return 参数值
     * @throws IllegalArgumentException 如果参数不存在
     */
    public String getRequired(String key) {
        String value = get(key);
        if (value == null) {
            throw new IllegalArgumentException("缺少必需的参数: " + key);
        }
        return value;
    }

    /**
     * 获取整数参数值
     *
     * @param key          键
     * @param defaultValue 默认值
     * @return 参数值，如果不存在或不是整数则返回默认值
     */
    public int getInt(String key, int defaultValue) {
        String value = get(key);
        if (value == null) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    /**
     * 获取长整型参数值
     *
     * @param key          键
     * @param defaultValue 默认值
     * @return 参数值，如果不存在或不是长整型则返回默认值
     */
    public long getLong(String key, long defaultValue) {
        String value = get(key);
        if (value == null) {
            return defaultValue;
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    /**
     * 获取双精度浮点数参数值
     *
     * @param key          键
     * @param defaultValue 默认值
     * @return 参数值，如果不存在或不是双精度浮点数则返回默认值
     */
    public double getDouble(String key, double defaultValue) {
        String value = get(key);
        if (value == null) {
            return defaultValue;
        }
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    /**
     * 获取布尔参数值
     *
     * @param key          键
     * @param defaultValue 默认值
     * @return 参数值，如果不存在则返回默认值
     */
    public boolean getBoolean(String key, boolean defaultValue) {
        String value = get(key);
        if (value == null) {
            return defaultValue;
        }
        return Boolean.parseBoolean(value) || "1".equals(value) || "yes".equalsIgnoreCase(value);
    }
}
