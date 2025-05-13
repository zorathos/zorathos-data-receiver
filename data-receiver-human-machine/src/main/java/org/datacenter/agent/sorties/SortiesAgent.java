package org.datacenter.agent.sorties;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.datacenter.agent.BaseAgent;
import org.datacenter.agent.util.KafkaUtil;
import org.datacenter.agent.util.SortiesHttpClientUtil;
import org.datacenter.config.HumanMachineConfig;
import org.datacenter.config.receiver.sorties.SortiesBatchReceiverConfig;
import org.datacenter.config.receiver.sorties.SortiesReceiverConfig;
import org.datacenter.exception.ZorathosException;
import org.datacenter.model.sorties.Sorties;
import org.datacenter.receiver.util.DataReceiverUtil;

import java.io.Serial;
import java.io.Serializable;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.datacenter.config.keys.HumanMachineSysConfigKey.AGENT_INTERVAL_SORTIES;
import static org.datacenter.config.keys.HumanMachineSysConfigKey.KAFKA_TOPIC_SORTIES;

/**
 * @author : [wangminan]
 * @description : 飞行架次Agent
 */
@Slf4j
public class SortiesAgent extends BaseAgent {

    private final ObjectMapper mapper = DataReceiverUtil.mapper;
    private ScheduledExecutorService scheduler;
    private final SortiesBatchReceiverConfig batchReceiverConfig;
    private final SortiesReceiverConfig sortiesReceiverConfig;

    public SortiesAgent(SortiesBatchReceiverConfig batchReceiverConfig, SortiesReceiverConfig sortiesReceiverConfig) {
        super();
        this.batchReceiverConfig = batchReceiverConfig;
        this.sortiesReceiverConfig = sortiesReceiverConfig;
    }

    @Override
    public void run() {
        super.run();
        if (!isStartedByThisInstance) {
            return;
        }

        log.info("Flight Agent start running, fetching data from server.");

        if (sortiesReceiverConfig.getRunMode().equals(SortiesReceiverConfig.RunMode.SCHEDULED)) {
            if (scheduler == null) {
                scheduler = Executors.newScheduledThreadPool(1);
            }

            scheduler.scheduleAtFixedRate(this::fetchSortiesFromRemote, 0, Integer.parseInt(
                    HumanMachineConfig.getProperty(AGENT_INTERVAL_SORTIES)), TimeUnit.SECONDS);
        } else if (sortiesReceiverConfig.getRunMode().equals(SortiesReceiverConfig.RunMode.AT_ONCE)) {
            // 只执行一次
            fetchSortiesFromRemote();
            Sorties sorties = Sorties.builder()
                    .sortieNumber(Sorties.END_SIGNAL_PREFIX + System.currentTimeMillis())
                    .build();
            // 发送结束消息
            try {
                KafkaUtil.sendMessage(
                        HumanMachineConfig.getProperty(KAFKA_TOPIC_SORTIES),
                        mapper.writeValueAsString(sorties));
            } catch (JsonProcessingException e) {
                throw new ZorathosException(e, "Error occurred while converting sorties to json.");
            }
        } else {
            log.error("Unknown run mode: {}", sortiesReceiverConfig.getRunMode());
            throw new ZorathosException("Unknown run mode: " + sortiesReceiverConfig.getRunMode());
        }
    }

    private void fetchSortiesFromRemote() {
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
