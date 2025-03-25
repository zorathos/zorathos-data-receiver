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
import org.datacenter.model.real.response.AssetTableConfigResult;
import org.datacenter.model.real.response.DataAssetResponse;
import org.datacenter.receiver.BaseReceiver;
import org.datacenter.receiver.util.JdbcSinkUtil;

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
            Connection connection = DriverManager.getConnection(
                    JdbcSinkUtil.TIDB_URL_SORTIES,
                    humanMachineProperties.getProperty("tidb.username"),
                    humanMachineProperties.getProperty("tidb.password"));
            String sql = "SELECT * FROM `%s` WHERE sortieName = ?".formatted(TiDBTable.SORTIES.getName());
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
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
        } catch (SQLException | ClassNotFoundException e) {
            throw new ZorathosException(e, "Error occurs while fetching sortie data from database.");
        }

        // 2. 获取到架次后拿着sortie的armType作为weaponNumber入参 icdVersion作为icd入参 查数据资产列表接口 获取资产列表
        log.info("Fetching asset list from web interface, armType: {}, icdVersion: {}.", armType, icdVersion);
        String assetListUrl = humanMachineProperties.getProperty("receiver.asset.host") +
                "/datahandle/asset/getObjectifyAsset?weaponModel=" + armType +
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

        // 4. 根据拉取到的数据异步地在数据库中创建表
        for (MutablePair<AssetSummary, List<AssetTableConfig>> assetConfigPair : assetResultList) {
            String dbName = TiDBDatabase.REAL_WORLD_FLIGHT.getName();
            // 我们还是用原来的表名
            String tableName = assetConfigPair.getKey().getFullName();
            // 生成 TiDB DDL
            StringBuilder createTableSql = new StringBuilder("CREATE TABLE `%s` (".formatted(tableName));
            List<AssetTableConfig> assetTableConfigs = assetConfigPair.getValue();

        }
    }

    @Override
    public void start() {
        // 4. 从Doris拉取数据入库
        log.info("Start to fetch real flight data from Doris and insert into database.");
        // 迭代遍历assetResultList
        for (MutablePair<AssetSummary, List<AssetTableConfig>> assetConfigPair : assetResultList) {
            String dbName = assetConfigPair.getKey().getDbName();
            String tableName = assetConfigPair.getKey().getFullName();
        }
    }

    /**
     * 接收数据资产数据
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
