package org.datacenter.agent.sorties;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.datacenter.agent.BaseAgent;
import org.datacenter.agent.util.KafkaUtil;
import org.datacenter.agent.util.SortiesHttpClientUtil;
import org.datacenter.model.sorties.Sorties;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.datacenter.config.system.BaseSysConfig.humanMachineProperties;

/**
 * @author : [wangminan]
 * @description : 飞行架次Agent
 */
@Slf4j
public class SortirsAgent extends BaseAgent {

    private final ObjectMapper mapper;
    private ScheduledExecutorService scheduler;

    public SortirsAgent() {
        super();
        this.mapper = new ObjectMapper();
    }

    @Override
    public void run() {
        super.run();
        log.info("Flight Agent start running, fetching data from server.");

        if (scheduler == null) {
            scheduler = Executors.newScheduledThreadPool(1);
        }

        scheduler.scheduleAtFixedRate(() -> {
            if (running) {
                // 1. 通过接口获取所有SortiesBatch
                List<Sorties> sortiesList = SortiesHttpClientUtil.getSortiesList();
                // 2. 转发到Kafka
                try {
                    String sortiesListInJson = mapper.writeValueAsString(sortiesList);
                    KafkaUtil.sendMessage(humanMachineProperties.getProperty("kafka.topic.sorties"), sortiesListInJson);
                } catch (Exception e) {
                    log.error("Failed to send message to Kafka, error: ", e);
                }
            }
        }, 0, Integer.parseInt(humanMachineProperties.getProperty("agent.interval.sorties")), TimeUnit.SECONDS);
    }

    @Override
    public void stop() {
        super.stop();
        scheduler.shutdown();
    }
}
