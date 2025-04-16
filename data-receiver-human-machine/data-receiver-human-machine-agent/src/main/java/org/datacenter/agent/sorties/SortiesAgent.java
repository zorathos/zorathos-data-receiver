package org.datacenter.agent.sorties;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;
import org.datacenter.agent.BaseAgent;
import org.datacenter.agent.util.KafkaUtil;
import org.datacenter.agent.util.ParameterUtil;
import org.datacenter.agent.util.SortiesHttpClientUtil;
import org.datacenter.config.HumanMachineConfig;
import org.datacenter.config.agent.sorties.SortiesAgentConfig;
import org.datacenter.config.agent.sorties.SortiesBatchAgentConfig;
import org.datacenter.exception.ZorathosException;
import org.datacenter.model.sorties.Sorties;

import java.util.Base64;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.datacenter.config.keys.HumanMachineAgentConfigKey.SORTIES_BASE_URL;
import static org.datacenter.config.keys.HumanMachineAgentConfigKey.SORTIES_BATCH_JSON;
import static org.datacenter.config.keys.HumanMachineAgentConfigKey.SORTIES_BATCH_URL;
import static org.datacenter.config.keys.HumanMachineSysConfigKey.AGENT_INTERVAL_SORTIES;
import static org.datacenter.config.keys.HumanMachineSysConfigKey.KAFKA_TOPIC_SORTIES;

/**
 * @author : [wangminan]
 * @description : 飞行架次Agent
 */
@Slf4j
public class SortiesAgent extends BaseAgent {

    private final ObjectMapper mapper;
    private ScheduledExecutorService scheduler;
    private final SortiesBatchAgentConfig batchAgentConfig;
    private final SortiesAgentConfig sortiesAgentConfig;

    public SortiesAgent(SortiesBatchAgentConfig batchAgentConfig, SortiesAgentConfig sortiesAgentConfig) {
        super();
        this.batchAgentConfig = batchAgentConfig;
        this.sortiesAgentConfig = sortiesAgentConfig;
        this.mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
    }

    @Override
    public void run() {
        super.run();
        if (!isStartedByThisInstance) {
            return;
        }

        log.info("Flight Agent start running, fetching data from server.");

        if (scheduler == null) {
            scheduler = Executors.newScheduledThreadPool(1);
        }

        scheduler.scheduleAtFixedRate(() -> {
            try {
                if (prepared) {
                    // 可以起flink任务
                    running = true;
                    // 1. 通过接口获取所有SortiesBatch
                    List<Sorties> sortiesList = SortiesHttpClientUtil.getSortiesList(batchAgentConfig, sortiesAgentConfig);
                    // 2. 转发到Kafka
                    List<CompletableFuture<Void>> futures = sortiesList.stream()
                            .map(sorties -> CompletableFuture.runAsync(() -> {
                                try {
                                    String flightPlanInJson = mapper.writeValueAsString(sorties);
                                    KafkaUtil.sendMessage(
                                            HumanMachineConfig.getProperty(KAFKA_TOPIC_SORTIES), flightPlanInJson);
                                } catch (JsonProcessingException e) {
                                    throw new ZorathosException(e, "Error occurred while converting sorties to json.");
                                }
                            }))
                            .toList();
                    CompletableFuture<Void> allFutures = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
                    try {
                        allFutures.join();
                    } catch (CompletionException e) {
                        throw new ZorathosException(e.getCause(), "Error in parallel processing of sorties.");
                    }
                }
            } catch (Exception e) {
                log.error("Error caught by scheduler pool. Task will be stopped.");
                stop();
            }
        }, 0, Integer.parseInt(
                HumanMachineConfig.getProperty(AGENT_INTERVAL_SORTIES)), TimeUnit.SECONDS);
    }

    @Override
    public void stop() {
        super.stop();
        try {
            scheduler.shutdown();
        } catch (Exception ex) {
            log.error("Error shutting down scheduler", ex);
        }
        System.exit(0);
    }

    public static void main(String[] args) {
        log.info("SortiesAgent starting.");
        ParameterUtil params = ParameterUtil.fromArgs(args);
        log.info("Params: {}", params.toMap());

        String encodedJson = params.getRequired(SORTIES_BATCH_JSON.getKeyForParamsMap());
        String decodedJson = new String(Base64.getDecoder().decode(encodedJson));

        SortiesBatchAgentConfig batchReceiverConfig = SortiesBatchAgentConfig.builder()
                .url(params.getRequired(SORTIES_BATCH_URL.getKeyForParamsMap()))
                .json(decodedJson)
                .build();
        SortiesAgentConfig sortiesAgentConfig = SortiesAgentConfig.builder()
                .baseUrl(params.getRequired(SORTIES_BASE_URL.getKeyForParamsMap()))
                .build();

        SortiesAgent sortiesAgent = new SortiesAgent(batchReceiverConfig, sortiesAgentConfig);
        try (ExecutorService executorService = Executors.newSingleThreadExecutor()) {
            executorService.submit(sortiesAgent);
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("Shutting down Sorties Agent...");
                sortiesAgent.stop();
            }));
        } catch (Exception e) {
            log.error("Error starting Sorties Agent", e);
            throw new ZorathosException(e, "Error starting Sorties Agent");
        }
    }
}
