package org.datacenter.receiver.equipment;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.datacenter.model.base.TiDBDatabase;
import org.datacenter.receiver.BaseReceiver;
import org.datacenter.receiver.util.DataReceiverUtil;
import org.datacenter.receiver.util.JdbcSinkUtil;

import java.text.MessageFormat;

import static org.apache.flink.table.api.Expressions.$;
import static org.datacenter.config.system.BaseSysConfig.humanMachineProperties;

/**
 * @author : [wangminan]
 * @description : 把plane_code迁移到本地的equipment_code表
 */
@Slf4j
public class EquipmentCodeCdcReceiver extends BaseReceiver {
    @Override
    public void prepare() {
        super.prepare();
    }

    @Override
    public void start() {
        StreamExecutionEnvironment env = DataReceiverUtil.prepareStreamEnv();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String sourceSql = MessageFormat.format("""
                        CREATE TABLE `equipment_code_source` (
                            `id` STRING,
                            `create_time` TIMESTAMP(3),
                            `create_people` STRING,
                            `update_time` TIMESTAMP(3),
                            `update_people` STRING,
                            `old_id` INT,
                            `plane_old_id` INT,
                            `c_coat` STRING,
                            `c_interior` STRING,
                            `c_manufacturing` STRING,
                            `icd_version` STRING,
                            `arionics_system_version` STRING,
                            `is_used` TINYINT,
                            `is_deleted` TINYINT,
                            `used_time` TIMESTAMP(3),
                            `icd_version_id` STRING,
                            PRIMARY KEY (id) NOT ENFORCED           -- 定义主键，但不强制执行
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
                humanMachineProperties.getProperty("receiver.equipment.mysql.username"),
                humanMachineProperties.getProperty("receiver.equipment.mysql.password"),
                humanMachineProperties.getProperty("receiver.equipment.mysql.database"),
                humanMachineProperties.getProperty("receiver.equipment.mysql.equipmentCode.table")
        );

        tableEnv.executeSql(sourceSql);

        String targetSql = MessageFormat.format("""
                        CREATE TABLE `equipment_code_target` (
                            `id` STRING COMMENT ''装备编号，主键 和 EquipmentInfo 中的 id 不是一个概念 id'',
                            `creator` STRING COMMENT ''创建人 create_people'',
                            `create_time` TIMESTAMP(3) COMMENT ''创建时间 create_time'',
                            `modifier` STRING COMMENT ''修改人 update_people'',
                            `modification_time` TIMESTAMP(3) COMMENT ''修改时间 update_time'',
                            `old_id` INT COMMENT ''老ID old_id'',
                            `plane_old_id` INT COMMENT ''飞机老ID plane_old_id'',
                            `c_coat` STRING COMMENT ''涂装号 c_coat'',
                            `c_interior` STRING COMMENT ''内部编号 c_interior'',
                            `c_manufacturing` STRING COMMENT ''出厂编号 c_manufacturing'',
                            `icd_version` STRING COMMENT ''icd版本 icd_version'',
                            `avionics_system_version` STRING COMMENT ''航电系统版本 avionics_system_version'',
                            `is_used` TINYINT COMMENT ''是否使用 is_used'',
                            `is_deleted` TINYINT COMMENT ''是否删除'',
                            `used_time` TIMESTAMP(3) COMMENT ''使用时间 yyyy-MM-dd used_time'',
                            `icd_version_id` STRING COMMENT ''icd_version_id'',
                            PRIMARY KEY (`id`) NOT ENFORCED
                        ) WITH (
                            ''connector'' = ''jdbc'',              -- 使用 JDBC 持久化
                            ''url'' = ''{0}'',                     -- TiDB 主机名
                            ''driver'' = ''{1}'',                  -- TiDB 端口
                            ''username'' = ''{2}'',                -- TiDB 用户名
                            ''password'' = ''{3}'',                -- TiDB 密码
                            ''table-name'' = ''{4}''               -- 表名
                        );
                        """,
                JdbcSinkUtil.TIDB_DATABASE_HUMAN_MACHINE,
                humanMachineProperties.getProperty("tidb.driverName"),
                humanMachineProperties.getProperty("tidb.username"),
                humanMachineProperties.getProperty("tidb.password"),
                humanMachineProperties.getProperty("receiver.equipment.tidb.equipmentCode.table")
        );

        tableEnv.executeSql(targetSql);

        Table sourceTable = tableEnv.from("equipment_code_source");

        sourceTable
                .select($("id"), $("create_people"), $("create_time"), $("update_people"), $("update_time"),
                        $("old_id"), $("plane_old_id"), $("c_coat"), $("c_interior"), $("c_manufacturing"),
                        $("icd_version"), $("arionics_system_version"), $("is_used"), $("is_deleted"), $("used_time"),
                        $("icd_version_id"))
                .as("id", "creator", "create_time", "modifier", "modification_time", "old_id", "plane_old_id",
                        "c_coat", "c_interior", "c_manufacturing", "icd_version", "avionics_system_version", "is_used",
                        "is_deleted", "used_time", "icd_version_id")
                .executeInsert("equipment_code_target");
    }

    public static void main(String[] args) {
        EquipmentCodeCdcReceiver receiver = new EquipmentCodeCdcReceiver();
        receiver.run();
    }
}
