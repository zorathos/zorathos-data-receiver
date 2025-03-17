package org.datacenter.agent.sorties;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.datacenter.agent.BaseAgent;
import org.datacenter.agent.util.KafkaUtil;
import org.datacenter.agent.util.SortiesHttpClientUtil;
import org.datacenter.model.sorties.SortiesBatch;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.datacenter.config.system.BaseSysConfig.humanMachineProperties;

/**
 * @author : [wangminan]
 * @description : 架次Batch Agent
 */
@Slf4j
public class SortiesBatchAgent extends BaseAgent {

    private final ObjectMapper mapper;
    private ScheduledExecutorService scheduler;

    public SortiesBatchAgent() {
        super();
        this.mapper = new ObjectMapper();
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
            if (prepared) {
                // 可以直接起flink
                running = true;
                // 1. 通过接口获取所有flightBatch
                List<SortiesBatch> flightBatchList = SortiesHttpClientUtil.getSortiesBatches();
                // 2. 转发到Kafka
                try {
                    String flightBatchListInJson = mapper.writeValueAsString(flightBatchList);
                    KafkaUtil.sendMessage(humanMachineProperties.getProperty("kafka.topic.sortiesBatch"), flightBatchListInJson);
                } catch (Exception e) {
                    log.error("Failed to send message to Kafka, error: ", e);
                }
            }
        }, 0, Integer.parseInt(humanMachineProperties.getProperty("agent.interval.sortiesBatch")), TimeUnit.SECONDS);
    }

    @Override
    public void stop() {
        super.stop();
        scheduler.shutdown();
    }
}
