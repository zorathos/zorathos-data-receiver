package org.datacenter.receiver.real;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLIndexDefinition;
import com.alibaba.druid.sql.dialect.doris.parser.DorisStatementParser;
import com.alibaba.druid.sql.dialect.starrocks.ast.statement.StarRocksCreateTableStatement;
import com.alibaba.druid.util.JdbcConstants;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.MutableTriple;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.datacenter.config.HumanMachineConfig;
import org.datacenter.config.receiver.real.AssetReceiverConfig;
import org.datacenter.exception.ZorathosException;
import org.datacenter.model.base.TiDBDatabase;
import org.datacenter.model.base.TiDBTable;
import org.datacenter.model.real.AssetSummary;
import org.datacenter.model.real.AssetTableConfig;
import org.datacenter.model.real.AssetTableModel;
import org.datacenter.model.real.AssetTableProperty;
import org.datacenter.model.real.response.AssetTableConfigResult;
import org.datacenter.model.real.response.DataAssetResponse;
import org.datacenter.receiver.BaseReceiver;
import org.datacenter.receiver.util.DataReceiverUtil;
import org.datacenter.receiver.util.JdbcSinkUtil;
import org.datacenter.util.MySQLDriverConnectionPool;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.datacenter.config.keys.HumanMachineSysConfigKey.TIDB_MYSQL_DRIVER_NAME;
import static org.datacenter.config.keys.HumanMachineSysConfigKey.TIDB_PASSWORD;
import static org.datacenter.config.keys.HumanMachineSysConfigKey.TIDB_USERNAME;

/**
 * @author : [wangminan]
 * @description : 通过JDBC连接器接收数据资产数据 这个任务是由整合触发的 一次性的
 */
@Data
@EqualsAndHashCode(callSuper = true)
@Slf4j
public class AssetJdbcReceiver extends BaseReceiver {

    private AssetReceiverConfig config;
    private String sortieNumber;
    private String sortieId;
    private String batchNumber;
    private ObjectMapper mapper = new ObjectMapper();
    // 本次读取过程中用到的 资产-资产表配置-DorisDDL triple本地缓存 这个pair的list肉眼可见的会占用很大的内存
    private List<MutableTriple<AssetSummary, List<AssetTableConfig>, StarRocksCreateTableStatement>> assetResultList = new ArrayList<>();

    /**
     * 准备阶段中需要完成 资产描述接入与目标库建表
     */
    @Override
    public void prepare() {
        super.prepare();
        mapper.registerModule(new JavaTimeModule());
        // 0. 连接池准备
        MySQLDriverConnectionPool sortieFlightPool = new MySQLDriverConnectionPool(TiDBDatabase.SORTIES);
        MySQLDriverConnectionPool realWorldFlightPool = new MySQLDriverConnectionPool(TiDBDatabase.REAL_WORLD_FLIGHT);

        try {
            // 1.  JDBC 根据架次号去查询架次数据
            String armType;
            String icdVersion;
            try {
                Class.forName(HumanMachineConfig.getProperty(TIDB_MYSQL_DRIVER_NAME));
                log.info("Fetching sortie data from database, sortieNumber: {}.", config.getSortieNumber());
                Connection sortiesConn = sortieFlightPool.getConnection();
                String sql = "SELECT * FROM `%s` WHERE sortie_number = ?".formatted(TiDBTable.SORTIES.getName());
                PreparedStatement preparedStatement = sortiesConn.prepareStatement(sql);
                preparedStatement.setString(1, config.getSortieNumber());
                ResultSet resultSet = preparedStatement.executeQuery();
                if (resultSet.next()) {
                    sortieId = resultSet.getString("sortie_id");
                    batchNumber = resultSet.getString("batch_number");
                    armType = resultSet.getString("arm_type");
                    icdVersion = resultSet.getString("icd_version");
                } else {
                    log.error("No sortie data found for sortieNumber: {}.", config.getSortieNumber());
                    throw new ZorathosException("No sortie data found for sortieNumber: " + config.getSortieNumber());
                }
                sortieFlightPool.returnConnection(sortiesConn);
            } catch (Exception e) {
                throw new ZorathosException(e, "Error occurs while fetching sortie data from database.");
            }

            // 2. 获取到架次后拿着sortie的armType作为weaponNumber入参 icdVersion作为icd入参 查数据资产列表接口 获取资产列表
            log.info("Fetching asset list from web interface, armType: {}, icdVersion: {}.", armType, icdVersion);
            String assetListUrl = config.getListBaseUrl() +
                    "?weaponModel=" + armType +
                    "&icd=" + icdVersion;
            List<AssetSummary> assetList;
            try (HttpClient client = HttpClient.newHttpClient()) {
                HttpRequest request = HttpRequest.newBuilder()
                        .GET()
                        .uri(new URI(assetListUrl))
                        .build();
                // 获取响应
                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                // 使用objectMapper 序列化返回列表
                String responseBody = response.body();
                DataAssetResponse dataAssetResponse = mapper.readValue(responseBody, DataAssetResponse.class);
                assetList = new ArrayList<>(dataAssetResponse.getResult());
            } catch (URISyntaxException | IOException | InterruptedException e) {
                throw new ZorathosException(e, "Error occurs while fetching asset list.");
            }

            // 3. 根据资产ID查资产配置信息接口
            for (AssetSummary asset : assetList) {
                Long assetId = asset.getId();
                String url = config.getConfigBaseUrl() + "?id=" + assetId;
                try (HttpClient client = HttpClient.newHttpClient()) {
                    HttpRequest request = HttpRequest.newBuilder()
                            .GET()
                            .uri(new URI(url))
                            .build();
                    // 获取响应
                    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                    // 使用objectMapper 序列化返回列表
                    String responseBody = response.body();
                    AssetTableConfigResult result = mapper.readValue(responseBody, AssetTableConfigResult.class);
                    assetResultList.add(new MutableTriple<>(asset, result.getResult(), new StarRocksCreateTableStatement()));
                } catch (URISyntaxException | IOException | InterruptedException e) {
                    throw new ZorathosException(e, "Error occurs while fetching asset config.");
                }
            }

            // 4.0 落库
            for (MutableTriple<AssetSummary, List<AssetTableConfig>, StarRocksCreateTableStatement> assetConfigPair : assetResultList) {
                // 4.1 把既有数据写入数据库
                AssetSummary assetSummary = assetConfigPair.getLeft();
                assetSummary.setSortieNumber(config.getSortieNumber());
                sinkAssetSummary(realWorldFlightPool, assetSummary);
                // 入库AssetTableConfig 以 AssetTableModel 和 AssetTableProperty 分别入库
                // 虽然这事情很荒谬 但我们确实只取第一个元素
                AssetTableConfig assetTableConfig = assetConfigPair.getMiddle().getFirst();
                // 先入库AssetTableModel
                AssetTableModel assetTableModel = assetTableConfig.getAssetModel();
                assetTableModel.setSortieNumber(config.getSortieNumber());
                sinkAssetTableModel(realWorldFlightPool, assetTableModel);
                // 再入库AssetTableProperty
                List<AssetTableProperty> propertyList = assetTableConfig.getPropertyList();
                for (AssetTableProperty assetTableProperty : propertyList) {
                    assetTableProperty.setModelId(assetTableModel.getId());
                    assetTableProperty.setSortieNumber(config.getSortieNumber());
                    sinkAssetTableProperty(realWorldFlightPool, assetTableProperty);
                }

                String dbName = assetConfigPair.getLeft().getDbName();
                String tableName = assetConfigPair.getLeft().getFullName();

                // 4.2 show create table 使用 druid 解 ddl 转发给TiDB
                try {
                    Connection dorisConn = DriverManager.getConnection(
                            "jdbc:mysql://%s/%s?useUnicode=true&characterEncoding=UTF-8&useSSL=false".formatted(config.getSqlNodes(), dbName),
                            config.getUsername(),
                            config.getPassword()
                    );
                    String sql = "SHOW CREATE TABLE %s".formatted(tableName);
                    PreparedStatement preparedStatement = dorisConn.prepareStatement(sql);
                    ResultSet resultSet = preparedStatement.executeQuery();
                    if (resultSet.next()) {
                        String createTableSql = resultSet.getString("Create Table");
                        log.info("Create table SQL for {}.{}: {}", dbName, tableName, createTableSql);
                        DorisStatementParser dorisStatementParser = new DorisStatementParser(createTableSql);
                        StarRocksCreateTableStatement sqlStatement =
                                (StarRocksCreateTableStatement) dorisStatementParser.getSQLCreateTableParser().parseStatement();
                        String tidbSql = dorisStatementToTiDBSql(tableName, sqlStatement);
                        createTableInTiDB(realWorldFlightPool, tidbSql);
                        assetConfigPair.setRight(sqlStatement);
                    } else {
                        throw new ZorathosException("No table found for %s.%s".formatted(dbName, tableName));
                    }
                    dorisConn.close();
                } catch (SQLException e) {
                    throw new ZorathosException(e, "Error occurs while connecting to doris database.");
                }
            }
        } finally {
            sortieFlightPool.closePool();
            realWorldFlightPool.closePool();
        }
    }

    private void sinkAssetSummary(MySQLDriverConnectionPool pool, AssetSummary summary) {
        Connection realConn = null;
        try {
            realConn = pool.getConnection();
            // 14个字段
            String sql = """
                    INSERT INTO %s (
                        id, sortie_number, name, full_name, model, icd_id, icd,
                        db_name, source, remark, objectify_flag,
                        copy_flag, labels, time_frame, time_type
                    ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                    ) ON DUPLICATE KEY UPDATE
                        name = VALUES(name),
                        sortie_number = VALUES(sortie_number),
                        full_name = VALUES(full_name),
                        model = VALUES(model),
                        icd_id = VALUES(icd_id),
                        icd = VALUES(icd),
                        db_name = VALUES(db_name),
                        source = VALUES(source),
                        remark = VALUES(remark),
                        objectify_flag = VALUES(objectify_flag),
                        copy_flag = VALUES(copy_flag),
                        labels = VALUES(labels),
                        time_frame = VALUES(time_frame),
                        time_type = VALUES(time_type)
                    """
                    .formatted(TiDBTable.ASSET_SUMMARY.getName());
            PreparedStatement preparedStatement = realConn.prepareStatement(sql);
            preparedStatement.setLong(1, summary.getId());
            preparedStatement.setString(2, summary.getSortieNumber());
            preparedStatement.setString(3, summary.getName());
            preparedStatement.setString(4, summary.getFullName());
            preparedStatement.setString(5, summary.getModel());
            preparedStatement.setInt(6, summary.getIcdId());
            preparedStatement.setString(7, summary.getIcd());
            preparedStatement.setString(8, summary.getDbName());
            preparedStatement.setShort(9, summary.getSource());
            preparedStatement.setString(10, summary.getRemark());
            preparedStatement.setInt(11, summary.getObjectifyFlag());
            preparedStatement.setInt(12, summary.getCopyFlag());
            preparedStatement.setString(13, summary.getLabels());
            preparedStatement.setInt(14, summary.getTimeFrame());
            preparedStatement.setInt(15, summary.getTimeType());
            preparedStatement.executeUpdate();
        } catch (Exception e) {
            throw new ZorathosException(e, "Error occurs while sinking asset summary.");
        } finally {
            pool.returnConnection(realConn);
        }
    }

    private void sinkAssetTableModel(MySQLDriverConnectionPool pool, AssetTableModel model) {
        Connection realConn = null;
        try {
            realConn = pool.getConnection();
            // 14个字段
            String sql = """
                    INSERT INTO %s (
                        id, sortie_number, name, asset_id, icd_id,
                        is_master, repeat_interval, repeat_times
                    ) VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?
                    ) ON DUPLICATE KEY UPDATE
                        name = VALUES(name),
                        sortie_number = VALUES(sortie_number),
                        asset_id = VALUES(asset_id),
                        icd_id = VALUES(icd_id),
                        is_master = VALUES(is_master),
                        repeat_interval = VALUES(repeat_interval),
                        repeat_times = VALUES(repeat_times)
                    """.formatted(TiDBTable.ASSET_TABLE_MODEL.getName());
            PreparedStatement preparedStatement = realConn.prepareStatement(sql);
            preparedStatement.setLong(1, model.getId());
            preparedStatement.setString(2, model.getSortieNumber());
            preparedStatement.setString(3, model.getName());
            preparedStatement.setLong(4, model.getAssetId());
            preparedStatement.setLong(5, model.getIcdId());
            preparedStatement.setInt(6, model.getIsMaster());
            preparedStatement.setInt(7, model.getRepeatInterval());
            preparedStatement.setInt(8, model.getRepeatTimes());
            preparedStatement.executeUpdate();
        } catch (Exception e) {
            throw new ZorathosException(e, "Error occurs while sinking asset table model.");
        } finally {
            pool.returnConnection(realConn);
        }
    }

    private void sinkAssetTableProperty(MySQLDriverConnectionPool pool, AssetTableProperty assetTableProperty) {
        Connection realConn = null;
        try {
            realConn = pool.getConnection();
            // 1. 删除所有modelId，sortieNumber，以及code相同的记录
            String deleteSql = """
                    DELETE FROM %s WHERE model_id = ? AND sortie_number = ? AND code = ?
                    """.formatted(TiDBTable.ASSET_TABLE_PROPERTY.getName());
            PreparedStatement deletePreparedStatement = realConn.prepareStatement(deleteSql);
            deletePreparedStatement.setLong(1, assetTableProperty.getModelId());
            deletePreparedStatement.setString(2, assetTableProperty.getSortieNumber());
            deletePreparedStatement.setString(3, assetTableProperty.getCode());
            deletePreparedStatement.executeUpdate();
            // 2. 插入新的记录
            String insertSql = """
                    INSERT INTO %s (
                        sortie_number, model_id, code,
                        name, type, is_time, two_d_display, label
                    ) VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?
                    ) ON DUPLICATE KEY UPDATE
                        name = VALUES(name),
                        model_id = VALUES(model_id),
                        code = VALUES(code),
                        type = VALUES(type),
                        is_time = VALUES(is_time),
                        two_d_display = VALUES(two_d_display),
                        label = VALUES(label)
                    """.formatted(TiDBTable.ASSET_TABLE_PROPERTY.getName());
            PreparedStatement insertStatement = realConn.prepareStatement(insertSql);
            insertStatement.setString(1, assetTableProperty.getSortieNumber());
            insertStatement.setLong(2, assetTableProperty.getModelId());
            insertStatement.setString(3, assetTableProperty.getCode());
            insertStatement.setString(4, assetTableProperty.getName());
            insertStatement.setString(5, assetTableProperty.getType());
            insertStatement.setInt(6, assetTableProperty.getIsTime());
            insertStatement.setInt(7, assetTableProperty.getTwoDDisplay());
            insertStatement.setString(8, assetTableProperty.getLabel());
            insertStatement.executeUpdate();
        } catch (Exception e) {
            throw new ZorathosException(e, "Error occurs while sinking table property.");
        } finally {
            pool.returnConnection(realConn);
        }
    }

    private void createTableInTiDB(MySQLDriverConnectionPool pool, String createTableDdl) {
        Connection realConn = null;
        try {
            realConn = pool.getConnection();
            PreparedStatement preparedStatement = realConn.prepareStatement(createTableDdl);
            preparedStatement.execute();
        } catch (Exception e) {
            throw new ZorathosException(e, "Error occurs while sinking asset table model.");
        } finally {
            pool.returnConnection(realConn);
        }
    }


    private String getColumnDefinitionsFromDorisStatement(StarRocksCreateTableStatement createTableStatement) {
        return createTableStatement.getTableElementList()
                .stream()
                .map(Object::toString)
                .collect(Collectors.joining(",\n"));
    }

    /**
     * 基于Druid parser 强转 doris sql 为 tidb sql
     *
     * @param tableName            表名
     * @param createTableStatement Doris的建表语句
     * @return TiDB的建表语句
     */
    private String dorisStatementToTiDBSql(String tableName, StarRocksCreateTableStatement createTableStatement) {
        SQLIndexDefinition indexDefinition = createTableStatement.getUnique().getIndexDefinition();
        String tidbSql = """
                CREATE TABLE IF NOT EXISTS %s (
                    %s,
                    PRIMARY KEY %s
                );
                """.formatted(
                tableName,
                getColumnDefinitionsFromDorisStatement(createTableStatement),
                indexDefinition
        );

        return SQLUtils.format(tidbSql, JdbcConstants.TIDB, SQLUtils.DEFAULT_LCASE_FORMAT_OPTION);
    }

    private String dorisStatementToFlinkSqlSource(String sourceDbName, String sourceTableName,
                                                  String flinkTableName, StarRocksCreateTableStatement createTableStatement) {
        SQLIndexDefinition indexDefinition = createTableStatement.getUnique().getIndexDefinition();
        String flinkSql = """
                CREATE TABLE IF NOT EXISTS %s (
                    %s,
                    PRIMARY KEY %s NOT ENFORCED
                ) WITH (
                    'connector' = 'doris',
                    'fenodes' = '%s',
                    'table.identifier' = '%s',
                    'username' = '%s',
                    'password' = '%s'
                );
                """.formatted(
                flinkTableName,
                getColumnDefinitionsFromDorisStatement(createTableStatement),
                indexDefinition,
                config.getFeNodes(),
                sourceDbName + "." + sourceTableName,
                config.getUsername(),
                config.getPassword()
        );

        return flinkSql
                .replaceAll("TEXT", "STRING")
                .replaceAll("text", "STRING")
                .replaceAll("VARCHAR\\(\\d+\\)", "STRING")
                .replaceAll("varchar\\(\\d+\\)", "STRING")
                .replaceAll("BIGINT\\(\\d+\\)", "BIGINT")
                .replaceAll("bigint\\(\\d+\\)", "BIGINT");
    }

    private String dorisStatementToFlinkSqlTarget(String targetTableName, String flinkTableName, StarRocksCreateTableStatement createTableStatement) {
        SQLIndexDefinition indexDefinition = createTableStatement.getUnique().getIndexDefinition();
        String flinkSql = """
                CREATE TABLE IF NOT EXISTS %s (
                    %s,
                    PRIMARY KEY %s NOT ENFORCED
                ) WITH (
                    'connector' = 'jdbc',             -- 使用 JDBC 持久化
                    'url' = '%s',                     -- TiDB 主机名
                    'driver' = '%s',                  -- TiDB 端口
                    'username' = '%s',                -- TiDB 用户名
                    'password' = '%s',                -- TiDB 密码
                    'table-name' = '%s'               -- 表名
                );
                """.formatted(
                flinkTableName,
                getColumnDefinitionsFromDorisStatement(createTableStatement),
                indexDefinition,
                JdbcSinkUtil.TIDB_REAL_WORLD_FLIGHT,
                HumanMachineConfig.getProperty(TIDB_MYSQL_DRIVER_NAME),
                HumanMachineConfig.getProperty(TIDB_USERNAME),
                HumanMachineConfig.getProperty(TIDB_PASSWORD),
                targetTableName
        );

        return flinkSql
                .replaceAll("TEXT", "STRING")
                .replaceAll("text", "STRING")
                .replaceAll("VARCHAR\\(\\d+\\)", "STRING")
                .replaceAll("varchar\\(\\d+\\)", "STRING")
                .replaceAll("BIGINT\\(\\d+\\)", "BIGINT")
                .replaceAll("bigint\\(\\d+\\)", "BIGINT");
    }


    /**
     * 数据库 多 source 多 sink
     */
    @Override
    public void start() {
        // 4. 从Doris拉取数据入库
        log.info("Start to fetch real flight data from Doris and insert into database.");
        StreamExecutionEnvironment env = DataReceiverUtil.prepareStreamEnv();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        StatementSet statementSet = tableEnv.createStatementSet();

        // 迭代遍历assetResultList
        for (int i = 0; i < assetResultList.size(); i++) {
            MutableTriple<AssetSummary, List<AssetTableConfig>, StarRocksCreateTableStatement> assetConfigPair = assetResultList.get(i);
            String dbName = assetConfigPair.getLeft().getDbName();
            String tableName = assetConfigPair.getLeft().getFullName();
            String flinkSource = "source_" + i;
            String flinkTarget = "target_" + i;

            String flinkSourceTable = dorisStatementToFlinkSqlSource(dbName, tableName, flinkSource, assetConfigPair.getRight());
            String flinkTargetTable = dorisStatementToFlinkSqlTarget(tableName, flinkTarget, assetConfigPair.getRight());

            tableEnv.executeSql(flinkSourceTable);
            tableEnv.executeSql(flinkTargetTable);

            Table sourceTable = tableEnv.from(flinkSource);
            statementSet.addInsert(flinkTarget, sourceTable);
        }

        // 执行所有语句
        try {
            statementSet.execute();
        } catch (Exception e) {
            throw new ZorathosException(e, "Error occurs while executing statement set.");
        }
    }

    /**
     * 接收数据资产数据
     *
     * @param args 接收参数 格式为
     *             --listBaseUrl http://192.168.10.100:8088/datahandle/asset/getObjectifyAsset
     *             --configBaseUrl http://192.168.10.100:8088/datahandle/asset/getAssetValidConfig
     *             --sortieNumber 20250303_五_01_ACT-3_邱陈_J16_07#02
     *             --feNodes 127.0.0.1:8030 --username root --password 123456
     */
    public static void main(String[] args) {
        ParameterTool params = ParameterTool.fromArgs(args);

        log.info("Params: {}", params.toMap());

        AssetReceiverConfig config = AssetReceiverConfig.builder()
                .listBaseUrl(params.getRequired("listBaseUrl"))
                .configBaseUrl(params.getRequired("configBaseUrl"))
                .sortieNumber(params.getRequired("sortieNumber"))
                .sqlNodes(params.getRequired("sqlNodes"))
                .feNodes(params.getRequired("feNodes"))
                .username(params.getRequired("username"))
                .password(params.get("password", null))
                .build();
        log.info("Start receiver data assets with sortieNumber: {}", config.getSortieNumber());
        AssetJdbcReceiver assetJdbcReceiver = new AssetJdbcReceiver();
        assetJdbcReceiver.setConfig(config);
        assetJdbcReceiver.run();
    }
}
