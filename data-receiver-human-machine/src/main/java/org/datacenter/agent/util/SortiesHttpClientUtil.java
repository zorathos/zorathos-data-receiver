package org.datacenter.agent.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.datacenter.exception.ZorathosException;
import org.datacenter.model.sorties.Sorties;
import org.datacenter.model.sorties.SortiesBatch;

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
    private static final String host;

    static {
        mapper = new ObjectMapper();
        host = humanMachineProperties.getProperty("agent.sorties.host");
    }

    /**
     * 从效能系统的架次系统中拉取架次批数据
     *
     * @return 架次批数据
     */
    public static List<SortiesBatch> getSortiesBatches() {
        String url = host + "/task/dataAsset/queryAllBatches";
        try (HttpClient client = HttpClient.newHttpClient()) {
            HttpRequest request = HttpRequest.newBuilder()
                    .header("Content-Type", "application/json")
                    // 请求体是固定的 就这两个参数
                    .POST(HttpRequest.BodyPublishers.ofString("""
                            {
                                "acmiTimeEnd": "",
                                "acmiTimeStart":""
                            
                            }
                            """))
                    .uri(new URI(url))
                    .build();
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            return mapper.readValue(response.body(), mapper.getTypeFactory()
                    .constructCollectionType(List.class, SortiesBatch.class));
        } catch (URISyntaxException | IOException | InterruptedException e) {
            throw new ZorathosException(e);
        }
    }

    /**
     * 根据架次批数据拉取具体的架次信息
     *
     * @return 架次信息
     */
    public static List<Sorties> getSortiesList() {
        List<Sorties> sortiesList = new ArrayList<>();
        List<SortiesBatch> sortiesBatchList = SortiesHttpClientUtil.getSortiesBatches();
        for (SortiesBatch sortiesBatch : sortiesBatchList) {
            String url = host + "/task/dataAsset/querySortiesByBatchId?batchId=" + sortiesBatch.getId();
            try (HttpClient client = HttpClient.newHttpClient()) {
                HttpRequest request = HttpRequest.newBuilder()
                        .header("Content-Type", "application/json")
                        // 请求体是固定的 就这两个参数
                        .GET()
                        .uri(new URI(url))
                        .build();
                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                sortiesList.addAll(mapper.readValue(response.body(), mapper.getTypeFactory()
                        .constructCollectionType(List.class, Sorties.class)));
            } catch (URISyntaxException | IOException | InterruptedException e) {
                throw new ZorathosException(e);
            }
        }
        return sortiesList;
    }
}
