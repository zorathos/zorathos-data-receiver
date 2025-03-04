package org.datacenter.agent.plan;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.datacenter.agent.BaseAgent;
import org.datacenter.agent.util.KafkaUtil;
import org.datacenter.agent.util.PersonnelAndFlightPlanHttpClientUtil;
import org.datacenter.config.plan.FlightPlanKafkaReceiverConfig;
import org.datacenter.exception.ZorathosException;
import org.datacenter.model.plan.FlightPlan;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.datacenter.config.system.BaseSysConfig.humanMachineProperties;

/**
 * @author : [wangminan]
 * @description : 通过飞行计划接口获取飞行计划数据 投递到Kafka
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Slf4j
@NoArgsConstructor
@AllArgsConstructor
public class FlightPlanAgent extends BaseAgent {

    private ObjectMapper mapper;
    private FlightPlanKafkaReceiverConfig receiverConfig;

    private ScheduledExecutorService scheduler;

    @Override
    public void run() {
        log.info("Flight plan agent start running, fetching data from flight agent system's xml interface and sending it to kafka.");

        if (scheduler == null) {
            scheduler = Executors.newScheduledThreadPool(1);
        }

        scheduler.scheduleAtFixedRate(() -> {
            if (running) {
                // 1. 获取请求
                List<FlightPlan> flightPlans = PersonnelAndFlightPlanHttpClientUtil.getFlightPlans();
                // 2. 转发到Kafka
                try {
                    String plansInJson = mapper.writeValueAsString(flightPlans);
                    KafkaUtil.sendMessage(humanMachineProperties
                            .getProperty("kafka.flightPlan.topic"), plansInJson);
                } catch (JsonProcessingException e) {
                    throw new ZorathosException(e, "Error occurs while converting flight plans to json string.");
                }
                // 3. 停5s
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    throw new ZorathosException(e, "Error occurs while sleeping.");
                }
            }
        }, 0,
                Integer.parseInt(humanMachineProperties.getProperty("agent.personnelAndFlightPlan.interval")),
                TimeUnit.MINUTES);
    }

    @Override
    public void stop() {
        running = false;
        scheduler.shutdown();
    }
}
