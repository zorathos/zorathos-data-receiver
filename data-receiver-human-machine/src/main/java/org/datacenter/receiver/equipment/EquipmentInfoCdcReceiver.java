package org.datacenter.receiver.equipment;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.datacenter.receiver.BaseReceiver;
import org.datacenter.receiver.util.DataReceiverUtil;
import org.datacenter.receiver.util.JdbcSinkUtil;

import java.text.MessageFormat;

import static org.apache.flink.table.api.Expressions.$;
import static org.datacenter.config.system.BaseSysConfig.humanMachineProperties;

/**
 * @author : [wangminan]
 * @description : 装备数据接收器 MySQL表 走CDC
 */
@Slf4j
public class EquipmentInfoCdcReceiver extends BaseReceiver {

    @Override
    public void prepare() {
        super.prepare();
    }

    @Override
    public void start() {
        StreamExecutionEnvironment env = DataReceiverUtil.prepareStreamEnv();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 1. 创建源表
        String sourceSql = MessageFormat.format("""
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
                            ''connector'' = ''mysql-cdc'',  -- 使用 MySQL 作为数据源
                            ''hostname'' = ''{0}'',   -- MySQL 主机名
                            ''port'' = ''{1}'',            -- MySQL 端口
                            ''username'' = ''{2}'',        -- MySQL 用户名
                            ''password'' = ''{3}'', -- MySQL 密码
                            ''database-name'' = ''{4}'', -- 数据库名
                            ''table-name'' = ''{5}'',       -- 表名
                            ''scan.startup.mode'' = ''initial'', -- 启动模式
                            ''scan.incremental.snapshot.enabled'' = ''true'', -- 启用增量快照
                            ''debezium.snapshot.mode'' = ''initial'', -- 快照模式
                            ''jdbc.properties.useSSL'' = ''false'' -- 不使用SSL
                        );
                        """,
                humanMachineProperties.getProperty("receiver.equipment.mysql.host"),
                humanMachineProperties.getProperty("receiver.equipment.mysql.port"),
                humanMachineProperties.get("receiver.equipment.mysql.username"),
                humanMachineProperties.get("receiver.equipment.mysql.password"),
                humanMachineProperties.getProperty("receiver.equipment.mysql.database"),
                humanMachineProperties.getProperty("receiver.equipment.mysql.equipmentInfo.table")
        );

        tableEnv.executeSql(sourceSql);

        // 2. 创建目标表
        String targetSql = MessageFormat.format("""
                        CREATE TABLE `equipment_info_target` (
                            `id` STRING COMMENT ''装备型号，主键 和 EquipmentCode 中的 equipmentNumber 不是一个概念 id'',
                            `create_time` TIMESTAMP(3) COMMENT ''创建时间'',
                            `creator` STRING COMMENT ''创建人'',
                            `modification_time` TIMESTAMP(3) COMMENT ''修改时间'',
                            `modifier` STRING COMMENT ''更新人'',
                            `old_id` INT COMMENT ''老id'',
                            `plane_type` STRING COMMENT ''飞机类型'',
                            `equipment_type` STRING COMMENT ''装备类型'',
                            `plane_weight` DOUBLE COMMENT ''飞机重量'',
                            `parent_id` STRING COMMENT ''父ID'',
                            `_3d_system` STRING COMMENT ''三维系统（1三型机，2体系，3小体系，4ACMI）'',
                            `is_deleted` TINYINT COMMENT ''是否删除（1删除，2未删除）'',
                            `equipment_model` STRING COMMENT ''装备模型'',
                            `unit` STRING COMMENT ''单位'',
                            `working_frequency_band` STRING COMMENT ''工作频段'',
                            `longitude` DOUBLE COMMENT ''经度'',
                            `latitude` DOUBLE COMMENT ''纬度'',
                            `is_3d_playback` TINYINT COMMENT ''是否三维回放（1代表是，2代表否）'',
                             PRIMARY KEY (id) NOT ENFORCED           -- 定义主键，但不强制执行
                        ) WITH (
                            ''connector'' = ''jdbc'',                -- 使用 JDBC 持久化
                            ''url'' = ''{0}'',                     -- TiDB 主机名
                            ''driver'' = ''{1}'',                  -- TiDB 端口
                            ''username'' = ''{2}'',                -- TiDB 用户名
                            ''password'' = ''{3}'',                -- TiDB 密码
                            ''table-name'' = ''{4}''               -- 表名
                        );
                        """,
                JdbcSinkUtil.TIDB_URL_HUMAN_MACHINE,
                humanMachineProperties.getProperty("tidb.driverName"),
                humanMachineProperties.getProperty("tidb.username"),
                humanMachineProperties.getProperty("tidb.password"),
                humanMachineProperties.getProperty("receiver.equipment.tidb.equipmentInfo.table")
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

    public static void main(String[] args) {
        EquipmentInfoCdcReceiver receiver = new EquipmentInfoCdcReceiver();
        receiver.run();
    }
}
