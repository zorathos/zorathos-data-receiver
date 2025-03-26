package org.datacenter.receiver.real;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.flink.api.java.utils.ParameterTool;
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
import org.datacenter.receiver.util.JdbcSinkUtil;
import org.datacenter.receiver.util.TiDBConnectionPool;

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

import static org.datacenter.config.system.BaseSysConfig.humanMachineProperties;

/**
 * @author : [wangminan]
 * @description : 通过JDBC连接器接收数据资产数据 这个任务是由整合触发的 一次性的
 */
@Data
@EqualsAndHashCode(callSuper = true)
@Slf4j
public class AssetJdbcReceiver extends BaseReceiver {

    private String sortieName;
    private String sortieId;
    private String batchId;
    private ObjectMapper mapper = new ObjectMapper();
    // 本次读取过程中用到的 资产-资产表配置 二元组本地缓存 这个pair的list肉眼可见的会占用很大的内存
    private List<MutablePair<AssetSummary, List<AssetTableConfig>>> assetResultList = new ArrayList<>();

    @Override
    public void prepare() {
        super.prepare();
        // 1.  JDBC 根据架次号去查询架次数据
        String armType;
        String icdVersion;
        try {
            log.info("Fetching sortie data from database, sortieName: {}.", sortieName);
            Class.forName(humanMachineProperties.getProperty("tidb.driverName"));
            Connection sortiesConn = DriverManager.getConnection(
                    JdbcSinkUtil.TIDB_URL_SORTIES,
                    humanMachineProperties.getProperty("tidb.username"),
                    humanMachineProperties.getProperty("tidb.password"));
            String sql = "SELECT * FROM `%s` WHERE sortieName = ?".formatted(TiDBTable.SORTIES.getName());
            PreparedStatement preparedStatement = sortiesConn.prepareStatement(sql);
            preparedStatement.setString(1, sortieName);
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                sortieId = resultSet.getString("sortie_id");
                batchId = resultSet.getString("batch_id");
                armType = resultSet.getString("arm_type");
                icdVersion = resultSet.getString("icd_version");
            } else {
                log.error("No sortie data found for sortieName: {}.", sortieName);
                throw new ZorathosException("No sortie data found for sortieName: " + sortieName);
            }
            sortiesConn.close();
        } catch (SQLException | ClassNotFoundException e) {
            throw new ZorathosException(e, "Error occurs while fetching sortie data from database.");
        }

        // 2. 获取到架次后拿着sortie的armType作为weaponNumber入参 icdVersion作为icd入参 查数据资产列表接口 获取资产列表
        log.info("Fetching asset list from web interface, armType: {}, icdVersion: {}.", armType, icdVersion);
        String assetListUrl = humanMachineProperties.getProperty("receiver.asset.host") + "/datahandle/asset/getObjectifyAsset?" +
                "weaponModel=" + armType +
                "&icd" + icdVersion;
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
            String url = humanMachineProperties.getProperty("receiver.asset.host") +
                    "/datahandle/asset/getAssetValidConfig?id=" + assetId;
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
                assetResultList.add(new MutablePair<>(asset, result.getResult()));
            } catch (URISyntaxException | IOException | InterruptedException e) {
                throw new ZorathosException(e, "Error occurs while fetching asset config.");
            }
        }

        // 4.0 初始化TiDB连接池
        TiDBConnectionPool pool = new TiDBConnectionPool(TiDBDatabase.REAL_WORLD_FLIGHT);

        // 4. 把既有数据写入数据库
        for (MutablePair<AssetSummary, List<AssetTableConfig>> assetSummaryListMutablePair : assetResultList) {
            AssetSummary summary = assetSummaryListMutablePair.getKey();
            sinkAssetSummary(pool, summary);
            // 入库AssetTableConfig 以 AssetTableModel 和 AssetTableProperty 分别入库
            // 虽然这事情很荒谬 但我们确实只取第一个元素
            AssetTableConfig assetTableConfig = assetSummaryListMutablePair.getValue().getFirst();
            // 先入库AssetTableModel
            AssetTableModel assetTableModel = assetTableConfig.getAssetModel();
            sinkAssetTableModel(pool, assetTableModel);
            // 再入库AssetTableProperty
            List<AssetTableProperty> propertyList = assetTableConfig.getPropertyList();
            for (AssetTableProperty assetTableProperty : propertyList) {
                sinkAssetTableProperty(pool, assetTableProperty);
            }
        }
    }

    private void sinkAssetSummary(TiDBConnectionPool pool, AssetSummary summary) {
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
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
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

    private void sinkAssetTableModel(TiDBConnectionPool pool, AssetTableModel model) {
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

    private void sinkAssetTableProperty(TiDBConnectionPool pool, AssetTableProperty assetTableProperty) {
        Connection realConn = null;
        try {
            realConn = pool.getConnection();
            String sql = """
                    INSERT INTO %s (
                        id, sortie_number, model_id, code,
                        name, type, is_time, two_d_display, label
                    ) VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?, ?
                    ) ON DUPLICATE KEY UPDATE
                        name = VALUES(name),
                        model_id = VALUES(model_id),
                        code = VALUES(code),
                        type = VALUES(type),
                        is_time = VALUES(is_time),
                        two_d_display = VALUES(two_d_display),
                        label = VALUES(label)
                    """.formatted(TiDBTable.ASSET_TABLE_PROPERTY.getName());
            PreparedStatement preparedStatement = realConn.prepareStatement(sql);
            preparedStatement.setLong(1, assetTableProperty.getId());
            preparedStatement.setString(2, assetTableProperty.getSortieNumber());
            preparedStatement.setLong(3, assetTableProperty.getModelId());
            preparedStatement.setLong(4, assetTableProperty.getCode());
            preparedStatement.setString(5, assetTableProperty.getName());
            preparedStatement.setString(6, assetTableProperty.getType());
            preparedStatement.setInt(7, assetTableProperty.getIsTime());
            preparedStatement.setInt(8, assetTableProperty.getTwoDDisplay());
            preparedStatement.setString(9, assetTableProperty.getLabel());
            preparedStatement.executeUpdate();
        } catch (Exception e) {
            throw new ZorathosException(e, "Error occurs while sinking table property.");
        } finally {
            pool.returnConnection(realConn);
        }
    }


    @Override
    public void start() {
        // 4. 从Doris拉取数据入库
        log.info("Start to fetch real flight data from Doris and insert into database.");
        /*
        CREATE TABLE ACMI_002_tspi_0305 (
            auto_id BIGINT,
            batch_id VARCHAR(255),
            sortie_id VARCHAR(255),
            code1 BIGINT,
            code2 BIGINT,
            code3 VARCHAR(255),
            code4 VARCHAR(255),
            code5 VARCHAR(255),
            code6 VARCHAR(255)
        )
        DUPLICATE KEY(auto_id, batch_id, sortie_id, code1)
        DISTRIBUTED BY HASH(auto_id) BUCKETS 10
        PROPERTIES (
            "replication_num" = "1"
        );
         */
        // 迭代遍历assetResultList
        for (MutablePair<AssetSummary, List<AssetTableConfig>> assetConfigPair : assetResultList) {
            String dbName = assetConfigPair.getKey().getDbName();
            String tableName = assetConfigPair.getKey().getFullName();
        }
    }

    /**
     * 接收数据资产数据
     *
     * @param args 接收参数 格式为 --sortieName 20250303_五_01_ACT-3_邱陈_J16_07#02
     */
    public static void main(String[] args) {
        ParameterTool params = ParameterTool.fromArgs(args);
        String sortieName = params.getRequired("sortieName");
        log.info("Start receiver data assets with sortieName: {}", sortieName);
        AssetJdbcReceiver assetJdbcReceiver = new AssetJdbcReceiver();
        assetJdbcReceiver.setSortieName(sortieName);
        assetJdbcReceiver.run();
    }
}
