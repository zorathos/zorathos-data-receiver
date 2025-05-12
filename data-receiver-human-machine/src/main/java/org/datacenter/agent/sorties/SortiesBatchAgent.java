package org.datacenter.agent.sorties;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.datacenter.agent.BaseAgent;
import org.datacenter.agent.util.KafkaUtil;
import org.datacenter.agent.util.SortiesHttpClientUtil;
import org.datacenter.config.HumanMachineConfig;
import org.datacenter.config.receiver.sorties.SortiesBatchReceiverConfig;
import org.datacenter.exception.ZorathosException;
import org.datacenter.model.sorties.Sorties;
import org.datacenter.model.sorties.SortiesBatch;
import org.datacenter.receiver.util.DataReceiverUtil;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.datacenter.config.keys.HumanMachineSysConfigKey.AGENT_INTERVAL_SORTIES_BATCH;
import static org.datacenter.config.keys.HumanMachineSysConfigKey.KAFKA_TOPIC_SORTIES_BATCH;

/**
 * @author : [wangminan]
 * @description : 架次Batch Agent
 */
@Slf4j
public class SortiesBatchAgent extends BaseAgent {

    private final ObjectMapper mapper = DataReceiverUtil.mapper;
    private ScheduledExecutorService scheduler;
    private final SortiesBatchReceiverConfig receiverConfig;

    public SortiesBatchAgent(SortiesBatchReceiverConfig receiverConfig) {
        super();
        this.receiverConfig = receiverConfig;
    }

    @Override
    public void run() {
        super.run();
        if (!isStartedByThisInstance) {
            return;
        }

        log.info("Flight Batch Agent start running, fetching data from server.");

        if (receiverConfig.getRunMode().equals(SortiesBatchReceiverConfig.RunMode.SCHEDULED)) {
            if (scheduler == null) {
                scheduler = Executors.newScheduledThreadPool(1);
            }
            scheduler.scheduleAtFixedRate(this::fetchSortiesBatchDataFromRemote,
                    0,
                    Integer.parseInt(HumanMachineConfig.getProperty(AGENT_INTERVAL_SORTIES_BATCH)), TimeUnit.SECONDS);
        } else if (receiverConfig.getRunMode().equals(SortiesBatchReceiverConfig.RunMode.AT_ONCE)) {
            // 直接获取一次
            fetchSortiesBatchDataFromRemote();
            SortiesBatch sortiesBatch = SortiesBatch.builder()
                    .batchNumber(Sorties.END_SIGNAL_PREFIX + System.currentTimeMillis())
                    .build();
            // 发送结束消息
            try {
                KafkaUtil.sendMessage(
                        HumanMachineConfig.getProperty(KAFKA_TOPIC_SORTIES_BATCH),
                        mapper.writeValueAsString(sortiesBatch));
            } catch (JsonProcessingException e) {
                throw new ZorathosException("Error occurred while converting sortiesBatch to json.");
            }
        } else {
            log.error("Unknown run mode: {}", receiverConfig.getRunMode());
            throw new ZorathosException("Unknown run mode: " + receiverConfig.getRunMode());
        }
    }

    private void fetchSortiesBatchDataFromRemote() {
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
                                KafkaUtil.sendMessage(HumanMachineConfig.getProperty(KAFKA_TOPIC_SORTIES_BATCH), sortiesBatchInJson);
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
    }

    @Override
    public void stop() {
        super.stop();
        if (!prepared && !running) {
            // 需要额外再做一次
            log.info("Personnel agent is not running, no need to stop.");
            return;
        }
        try {
            scheduler.shutdown();
        } catch (Exception ex) {
            log.error("Error shutting down scheduler", ex);
        }
        System.exit(0);
    }
}
