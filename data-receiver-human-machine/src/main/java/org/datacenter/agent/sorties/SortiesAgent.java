package org.datacenter.agent.sorties;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;
import org.datacenter.agent.BaseAgent;
import org.datacenter.agent.util.KafkaUtil;
import org.datacenter.agent.util.SortiesHttpClientUtil;
import org.datacenter.config.sorties.SortiesBatchReceiverConfig;
import org.datacenter.config.sorties.SortiesReceiverConfig;
import org.datacenter.exception.ZorathosException;
import org.datacenter.model.sorties.Sorties;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.datacenter.config.system.BaseSysConfig.humanMachineProperties;

/**
 * @author : [wangminan]
 * @description : 飞行架次Agent
 */
@Slf4j
public class SortiesAgent extends BaseAgent {

    private final ObjectMapper mapper;
    private ScheduledExecutorService scheduler;
    private final SortiesBatchReceiverConfig batchReceiverConfig;
    private final SortiesReceiverConfig sortiesReceiverConfig;

    public SortiesAgent(SortiesBatchReceiverConfig batchReceiverConfig, SortiesReceiverConfig sortiesReceiverConfig) {
        super();
        this.batchReceiverConfig = batchReceiverConfig;
        this.sortiesReceiverConfig = sortiesReceiverConfig;
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
                    List<Sorties> sortiesList = SortiesHttpClientUtil.getSortiesList(batchReceiverConfig, sortiesReceiverConfig);
                    // 2. 转发到Kafka
                    List<CompletableFuture<Void>> futures = sortiesList.stream()
                            .map(sorties -> CompletableFuture.runAsync(() -> {
                                try {
                                    String flightPlanInJson = mapper.writeValueAsString(sorties);
                                    KafkaUtil.sendMessage(humanMachineProperties.getProperty("kafka.topic.sorties"), flightPlanInJson);
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
        }, 0, Integer.parseInt(humanMachineProperties.getProperty("agent.interval.sorties")), TimeUnit.SECONDS);
    }

    @Override
    public void stop() {
        super.stop();
        try {
            scheduler.shutdown();
        } catch (Exception ex) {
            log.error("Error shutting down scheduler", ex);
        }
    }
}
