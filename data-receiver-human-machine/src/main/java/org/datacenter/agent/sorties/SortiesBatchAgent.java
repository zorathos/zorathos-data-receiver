package org.datacenter.agent.sorties;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;
import org.datacenter.agent.BaseAgent;
import org.datacenter.agent.util.KafkaUtil;
import org.datacenter.agent.util.SortiesHttpClientUtil;
import org.datacenter.config.sorties.SortiesBatchReceiverConfig;
import org.datacenter.config.system.HumanMachineSysConfig;
import org.datacenter.exception.ZorathosException;
import org.datacenter.model.sorties.SortiesBatch;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author : [wangminan]
 * @description : 架次Batch Agent
 */
@Slf4j
public class SortiesBatchAgent extends BaseAgent {

    private final ObjectMapper mapper;
    private ScheduledExecutorService scheduler;
    private final SortiesBatchReceiverConfig receiverConfig;

    public SortiesBatchAgent(SortiesBatchReceiverConfig receiverConfig) {
        super();
        this.receiverConfig = receiverConfig;
        this.mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
    }

    @Override
    public void run() {
        super.run();
        if (!isStartedByThisInstance) {
            return;
        }

        log.info("Flight Batch Agent start running, fetching data from server.");

        if (scheduler == null) {
            scheduler = Executors.newScheduledThreadPool(1);
        }

        scheduler.scheduleAtFixedRate(() -> {
            try {
                if (prepared) {
                    // 可以直接起flink
                    running = true;
                    // 1. 通过接口获取所有flightBatch
                    List<SortiesBatch> flightBatchList = SortiesHttpClientUtil.getSortiesBatches(receiverConfig);
                    // 2. 转发到Kafka
                    List<CompletableFuture<Void>> futures = flightBatchList.stream()
                            .map(sortiesBatch -> CompletableFuture.runAsync(() -> {
                                try {
                                    String sortiesBatchInJson = mapper.writeValueAsString(sortiesBatch);
                                    KafkaUtil.sendMessage(HumanMachineSysConfig.getHumanMachineProperties().getProperty("kafka.topic.sortiesBatch"), sortiesBatchInJson);
                                } catch (JsonProcessingException e) {
                                    throw new ZorathosException(e, "Error occurred while converting sortiesBatch to json.");
                                }
                            }))
                            .toList();
                    CompletableFuture<Void> allFutures = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
                    try {
                        allFutures.join();
                    } catch (CompletionException e) {
                        throw new ZorathosException(e.getCause(), "Error in parallel processing of sortiesBatch.");
                    }
                }
            } catch (ZorathosException e) {
                log.error("Error caught by scheduler pool. Task will be stopped.");
                stop();
            }
        }, 0, Integer.parseInt(HumanMachineSysConfig.getHumanMachineProperties().getProperty("agent.interval.sortiesBatch")), TimeUnit.SECONDS);
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
}
