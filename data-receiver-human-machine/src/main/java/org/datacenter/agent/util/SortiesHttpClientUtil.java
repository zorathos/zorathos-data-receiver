package org.datacenter.agent.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.datacenter.config.sorties.SortiesBatchReceiverConfig;
import org.datacenter.config.sorties.SortiesReceiverConfig;
import org.datacenter.exception.ZorathosException;
import org.datacenter.model.sorties.Sorties;
import org.datacenter.model.sorties.SortiesBatch;
import org.datacenter.model.sorties.response.SortiesBatchResponse;
import org.datacenter.model.sorties.response.SortiesResponse;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;

import static org.datacenter.config.system.BaseSysConfig.humanMachineProperties;

/**
 * @author : [wangminan]
 * @description : 架次信息用的HttpClient工具类
 */
public class SortiesHttpClientUtil {

    private static final ObjectMapper mapper;

    static {
        mapper = new ObjectMapper();
    }

    /**
     * 从效能系统的架次系统中拉取架次批数据
     *
     * @return 架次批数据
     */
    public static List<SortiesBatch> getSortiesBatches(SortiesBatchReceiverConfig receiverConfig) {
        String url = receiverConfig.getSortiesBatchUrl();
        try (HttpClient client = HttpClient.newHttpClient()) {
            HttpRequest request = HttpRequest.newBuilder()
                    .header("Content-Type", "application/json")
                    // 请求体是固定的 就这两个参数
                    .POST(HttpRequest.BodyPublishers.ofString(receiverConfig.getSortiesBatchJson()))
                    .uri(new URI(url))
                    .build();
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            return mapper.readValue(response.body(), SortiesBatchResponse.class).getData();
        } catch (URISyntaxException | IOException | InterruptedException e) {
            throw new ZorathosException(e);
        }
    }

    /**
     * 根据架次批数据拉取具体的架次信息
     *
     * @return 架次信息
     */
    public static List<Sorties> getSortiesList(SortiesBatchReceiverConfig sortiesBatchReceiverConfig,
                                               SortiesReceiverConfig sortiesReceiverConfig) {
        List<Sorties> sortiesList = new ArrayList<>();
        // 拿着batch号去找对应的sorties
        List<SortiesBatch> sortiesBatchList = SortiesHttpClientUtil.getSortiesBatches(sortiesBatchReceiverConfig);
        for (SortiesBatch sortiesBatch : sortiesBatchList) {
            String url = sortiesReceiverConfig.getSortiesBaseUrl() + "?batchId=" + sortiesBatch.getId();
            try (HttpClient client = HttpClient.newHttpClient()) {
                HttpRequest request = HttpRequest.newBuilder()
                        .header("Content-Type", "application/json")
                        // 请求体是固定的 就这两个参数
                        .GET()
                        .uri(new URI(url))
                        .build();
                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                sortiesList.addAll(mapper.readValue(response.body(), SortiesResponse.class).getData());
            } catch (URISyntaxException | IOException | InterruptedException e) {
                throw new ZorathosException(e);
            }
        }
        return sortiesList;
    }
}
