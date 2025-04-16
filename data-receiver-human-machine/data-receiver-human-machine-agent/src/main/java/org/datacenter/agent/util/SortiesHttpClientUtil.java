package org.datacenter.agent.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;
import org.datacenter.config.HumanMachineConfig;
import org.datacenter.config.agent.sorties.SortiesAgentConfig;
import org.datacenter.config.agent.sorties.SortiesBatchAgentConfig;
import org.datacenter.exception.ZorathosException;
import org.datacenter.model.sorties.Sorties;
import org.datacenter.model.sorties.SortiesBatch;
import org.datacenter.model.sorties.response.SortiesBatchResponse;
import org.datacenter.model.sorties.response.SortiesResponse;
import org.datacenter.util.RetryUtil;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.util.ArrayList;
import java.util.List;

import static org.datacenter.config.keys.HumanMachineSysConfigKey.AGENT_RETRIES_HTTP;

/**
 * @author : [wangminan]
 * @description : 架次信息用的HttpClient工具类
 */
@Slf4j
public class SortiesHttpClientUtil {

    private static final ObjectMapper mapper;
    private static final Integer MAX_RETRY_COUNT = Integer.parseInt(HumanMachineConfig.getProperty(AGENT_RETRIES_HTTP));

    static {
        mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
    }

    /**
     * 从效能系统的架次系统中拉取架次批数据
     *
     * @return 架次批数据
     */
    public static List<SortiesBatch> getSortiesBatches(SortiesBatchAgentConfig receiverConfig) {
        String url = receiverConfig.getUrl();
        try (HttpClient client = HttpClient.newHttpClient()) {
            return RetryUtil.executeHttpRequestWithRetry(
                    () -> {
                        try {
                            return HttpRequest.newBuilder()
                                    .header("Content-Type", "application/json")
                                    .POST(HttpRequest.BodyPublishers.ofString(receiverConfig.getJson()))
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
    public static List<Sorties> getSortiesList(SortiesBatchAgentConfig SortiesBatchAgentConfig,
                                               SortiesAgentConfig sortiesAgentConfig) {
        List<Sorties> sortiesList = new ArrayList<>();
        // 拿着batch号去找对应的sorties
        List<SortiesBatch> sortiesBatchList = SortiesHttpClientUtil.getSortiesBatches(SortiesBatchAgentConfig);

        for (SortiesBatch sortiesBatch : sortiesBatchList) {
            final String url = sortiesAgentConfig.getBaseUrl() + "?batchId=" + sortiesBatch.getId();
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
