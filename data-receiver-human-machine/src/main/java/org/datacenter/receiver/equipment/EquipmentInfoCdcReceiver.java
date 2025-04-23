package org.datacenter.receiver.equipment;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.datacenter.config.HumanMachineConfig;
import org.datacenter.config.receiver.equipment.EquipmentInfoReceiverConfig;
import org.datacenter.receiver.BaseReceiver;
import org.datacenter.receiver.util.DataReceiverUtil;
import org.datacenter.receiver.util.JdbcSinkUtil;

import static org.apache.flink.table.api.Expressions.$;
import static org.datacenter.config.keys.HumanMachineReceiverConfigKey.EQUIPMENT_INFO_DATABASE;
import static org.datacenter.config.keys.HumanMachineReceiverConfigKey.EQUIPMENT_INFO_HOST;
import static org.datacenter.config.keys.HumanMachineReceiverConfigKey.EQUIPMENT_INFO_PASSWORD;
import static org.datacenter.config.keys.HumanMachineReceiverConfigKey.EQUIPMENT_INFO_PORT;
import static org.datacenter.config.keys.HumanMachineReceiverConfigKey.EQUIPMENT_INFO_TABLE;
import static org.datacenter.config.keys.HumanMachineReceiverConfigKey.EQUIPMENT_INFO_USERNAME;
import static org.datacenter.config.keys.HumanMachineSysConfigKey.RECEIVER_EQUIPMENT_TIDB_EQUIPMENT_INFO_TABLE;
import static org.datacenter.config.keys.HumanMachineSysConfigKey.TIDB_MYSQL_DRIVER_NAME;
import static org.datacenter.config.keys.HumanMachineSysConfigKey.TIDB_PASSWORD;
import static org.datacenter.config.keys.HumanMachineSysConfigKey.TIDB_USERNAME;

/**
 * @author : [wangminan]
 * @description : 装备数据接收器 MySQL表 走CDC
 */
@Slf4j
public class EquipmentInfoCdcReceiver extends BaseReceiver {

    private final EquipmentInfoReceiverConfig config;

    public EquipmentInfoCdcReceiver(EquipmentInfoReceiverConfig config) {
        this.config = config;
    }

    @Override
    public void prepare() {
        super.prepare();
    }

    @Override
    public void start() {
        StreamExecutionEnvironment env = DataReceiverUtil.prepareStreamEnv();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 1. 创建源表
        String sourceSql = """
                CREATE TABLE `equipment_info_source` (
                    `id` STRING,
                    `create_time` TIMESTAMP(3),
                    `create_people` STRING,
                    `update_time` TIMESTAMP(3),
                    `update_people` STRING,
                    `old_id` INT,
                    `plane_type` STRING,
                    `equipment_type` STRING,
                    `plane_weight` DECIMAL(38, 10),
                    `parent_id` STRING,
                    `threetype_system` CHAR(1),
                    `is_deleted` TINYINT,
                    `equipment_model` STRING,
                    `unit` STRING,
                    `work_band` STRING,
                    `longitude` STRING,
                    `latitude` STRING,
                    `is_three_back` TINYINT,
                    PRIMARY KEY (id) NOT ENFORCED
                ) WITH (
                    'connector' = 'mysql-cdc',  -- 使用 MySQL 作为数据源
                    'hostname' = '%s',   -- MySQL 主机名
                    'port' = '%s',            -- MySQL 端口
                    'username' = '%s',        -- MySQL 用户名
                    'password' = '%s', -- MySQL 密码
                    'database-name' = '%s', -- 数据库名
                    'table-name' = '%s',       -- 表名
                    'scan.startup.mode' = 'initial', -- 启动模式
                    'scan.incremental.snapshot.enabled' = 'true', -- 启用增量快照
                    'debezium.snapshot.mode' = 'initial', -- 快照模式
                    'jdbc.properties.useSSL' = 'false' -- 不使用SSL
                );
                """.formatted(
                config.getHost(),
                config.getPort(),
                config.getUsername(),
                config.getPassword(),
                config.getDatabase(),
                config.getTable()
        );

        tableEnv.executeSql(sourceSql);

        // 2. 创建目标表
        String targetSql = """
                CREATE TABLE `equipment_info_target` (
                    `id` STRING COMMENT '装备型号，主键 和 EquipmentCode 中的 equipmentNumber 不是一个概念 id',
                    `create_time` TIMESTAMP(3) COMMENT '创建时间',
                    `creator` STRING COMMENT '创建人',
                    `modification_time` TIMESTAMP(3) COMMENT '修改时间',
                    `modifier` STRING COMMENT '更新人',
                    `old_id` INT COMMENT '老id',
                    `plane_type` STRING COMMENT '飞机类型',
                    `equipment_type` STRING COMMENT '装备类型',
                    `plane_weight` DOUBLE COMMENT '飞机重量',
                    `parent_id` STRING COMMENT '父ID',
                    `_3d_system` STRING COMMENT '三维系统（1三型机，2体系，3小体系，4ACMI）',
                    `is_deleted` TINYINT COMMENT '是否删除（1删除，2未删除）',
                    `equipment_model` STRING COMMENT '装备模型',
                    `unit` STRING COMMENT '单位',
                    `working_frequency_band` STRING COMMENT '工作频段',
                    `longitude` DOUBLE COMMENT '经度',
                    `latitude` DOUBLE COMMENT '纬度',
                    `is_3d_playback` TINYINT COMMENT '是否三维回放（1代表是，2代表否）',
                     PRIMARY KEY (id) NOT ENFORCED           -- 定义主键，但不强制执行
                ) WITH (
                    'connector' = 'jdbc',                -- 使用 JDBC 持久化
                    'url' = '%s',                     -- TiDB 主机名
                    'driver' = '%s',                  -- TiDB 端口
                    'username' = '%s',                -- TiDB 用户名
                    'password' = '%s',                -- TiDB 密码
                    'table-name' = '%s'               -- 表名
                );
                """.formatted(
                JdbcSinkUtil.TIDB_URL_EQUIPMENT,
                HumanMachineConfig.getProperty(TIDB_MYSQL_DRIVER_NAME),
                HumanMachineConfig.getProperty(TIDB_USERNAME),
                HumanMachineConfig.getProperty(TIDB_PASSWORD),
                HumanMachineConfig.getProperty(RECEIVER_EQUIPMENT_TIDB_EQUIPMENT_INFO_TABLE)
        );

        tableEnv.executeSql(targetSql);

        Table sourceTable = tableEnv.from("equipment_info_source");

        sourceTable
                .select($("id"), $("create_time"), $("create_people"), $("update_time"), $("update_people"),
                        $("old_id"), $("plane_type"), $("equipment_type"), $("plane_weight"), $("parent_id"),
                        $("threetype_system"), $("is_deleted"), $("equipment_model"), $("unit"), $("work_band"),
                        $("longitude").cast(DataTypes.DOUBLE()), $("latitude").cast(DataTypes.DOUBLE()), $("is_three_back"))
                .as("id", "create_time", "creator", "modification_time", "modifier",
                        "old_id", "plane_type", "equipment_type", "plane_weight", "parent_id",
                        "_3d_system", "is_deleted", "equipment_model", "unit", "working_frequency_band",
                        "longitude", "latitude", "is_3d_playback")
                .executeInsert("equipment_info_target");
    }

    /**
     * 主函数
     * @param args 入参
     * --host 24.47.153.151  --port 3306 --username root --password 123456 --database basedb --table plane_code
     */
    public static void main(String[] args) {
        ParameterTool params = ParameterTool.fromArgs(args);
        EquipmentInfoReceiverConfig config = EquipmentInfoReceiverConfig.builder()
                .host(params.getRequired(EQUIPMENT_INFO_HOST.getKeyForParamsMap()))
                .port(params.getRequired(EQUIPMENT_INFO_PORT.getKeyForParamsMap()))
                .database(params.getRequired(EQUIPMENT_INFO_DATABASE.getKeyForParamsMap()))
                .username(params.getRequired(EQUIPMENT_INFO_USERNAME.getKeyForParamsMap()))
                .password(params.getRequired(EQUIPMENT_INFO_PASSWORD.getKeyForParamsMap()))
                .table(params.getRequired(EQUIPMENT_INFO_TABLE.getKeyForParamsMap()))
                .build();
        EquipmentInfoCdcReceiver receiver = new EquipmentInfoCdcReceiver(config);
        receiver.run();
    }
}
