package org.datacenter.agent.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;
import org.datacenter.config.sorties.SortiesBatchReceiverConfig;
import org.datacenter.config.sorties.SortiesReceiverConfig;
import org.datacenter.config.HumanMachineSysConfig;
import org.datacenter.exception.ZorathosException;
import org.datacenter.model.sorties.Sorties;
import org.datacenter.model.sorties.SortiesBatch;
import org.datacenter.model.sorties.response.SortiesBatchResponse;
import org.datacenter.model.sorties.response.SortiesResponse;
import org.datacenter.receiver.util.RetryUtil;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.util.ArrayList;
import java.util.List;

/**
 * @author : [wangminan]
 * @description : 架次信息用的HttpClient工具类
 */
@Slf4j
public class SortiesHttpClientUtil {

    private static final ObjectMapper mapper;
    private static final Integer MAX_RETRY_COUNT = Integer.parseInt(HumanMachineSysConfig.getHumanMachineProperties().getProperty("agent.retries.http"));

    static {
        mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
    }

    /**
     * 从效能系统的架次系统中拉取架次批数据
     *
     * @return 架次批数据
     */
    public static List<SortiesBatch> getSortiesBatches(SortiesBatchReceiverConfig receiverConfig) {
        String url = receiverConfig.getSortiesBatchUrl();
        try (HttpClient client = HttpClient.newHttpClient()) {
            return RetryUtil.executeHttpRequestWithRetry(
                    () -> {
                        try {
                            return HttpRequest.newBuilder()
                                    .header("Content-Type", "application/json")
                                    .POST(HttpRequest.BodyPublishers.ofString(receiverConfig.getSortiesBatchJson()))
                                    .uri(new URI(url))
                                    .build();
                        } catch (URISyntaxException e) {
                            throw new ZorathosException(e, "Failed to create sorties batch URL");
                        }
                    },
                    client,
                    MAX_RETRY_COUNT,
                    response -> {
                        if (response.statusCode() != 200) {
                            log.error("Error occurs while fetching sorties batches, response code: {}, response body: {}",
                                    response.statusCode(), response.body());
                            throw new ZorathosException("Error occurs while fetching sorties batches.");
                        }
                        try {
                            return mapper.readValue(response.body(), SortiesBatchResponse.class).getData();
                        } catch (Exception e) {
                            throw new ZorathosException(e, "Error parsing sorties batch response");
                        }
                    }
            );
        } catch (Exception e) {
            throw new ZorathosException(e, "Error occurs while fetching sorties batches.");
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
            final String url = sortiesReceiverConfig.getSortiesBaseUrl() + "?batchId=" + sortiesBatch.getId();
            try (HttpClient client = HttpClient.newHttpClient()) {
                List<Sorties> batchSorties = RetryUtil.executeHttpRequestWithRetry(
                        () -> {
                            try {
                                return HttpRequest.newBuilder()
                                        .header("Content-Type", "application/json")
                                        .GET()
                                        .uri(new URI(url))
                                        .build();
                            } catch (URISyntaxException e) {
                                throw new ZorathosException(e, "Failed to create sorties URL");
                            }
                        },
                        client,
                        MAX_RETRY_COUNT,
                        response -> {
                            if (response.statusCode() != 200) {
                                log.error("Error occurs while fetching sorties, response code: {}, response body: {}",
                                        response.statusCode(), response.body());
                                throw new ZorathosException("Error occurs while fetching sorties.");
                            }
                            try {
                                return mapper.readValue(response.body(), SortiesResponse.class).getData();
                            } catch (Exception e) {
                                throw new ZorathosException(e, "Error parsing sorties response");
                            }
                        }
                );
                sortiesList.addAll(batchSorties);
            } catch (Exception e) {
                throw new ZorathosException(e, "Error occurs while fetching sorties for batch: " + sortiesBatch.getId());
            }
        }
        return sortiesList;
    }
}
