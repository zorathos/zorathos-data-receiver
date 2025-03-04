package org.datacenter.agent.personnel;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.datacenter.agent.BaseAgent;
import org.datacenter.agent.util.KafkaUtil;
import org.datacenter.agent.util.PersonnelAndFlightPlanHttpClientUtil;
import org.datacenter.config.plan.FlightPlanKafkaReceiverConfig;
import org.datacenter.exception.ZorathosException;
import org.datacenter.model.crew.PersonnelInfo;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.datacenter.config.system.BaseSysConfig.humanMachineProperties;

/**
 * @author : [wangminan]
 * @description : 人员信息拉取Agent
 */
@Slf4j
public class PersonnelAgent extends BaseAgent {

    private ObjectMapper mapper;
    private FlightPlanKafkaReceiverConfig receiverConfig;

    private ScheduledExecutorService scheduler;

    @Override
    public void run() {
        super.run();
        log.info("Personnel agent start running, fetching data from personnel agent system's json interface and sending it to kafka.");
        scheduler.scheduleAtFixedRate(() -> {
                if (running) {
                    // 1. 拉取人员数据
                    List<PersonnelInfo> personnelInfos = PersonnelAndFlightPlanHttpClientUtil.getPersonnelInfos();
                    // 2. 转发到Kafka
                    try {
                        String personnelInfosInJson = mapper.writeValueAsString(personnelInfos);
                        KafkaUtil.sendMessage(humanMachineProperties
                                .getProperty("kafka.topic.personnel"), personnelInfosInJson);
                    } catch (JsonProcessingException e) {
                        throw new ZorathosException(e, "Error occurs while converting personnel infos to json string.");
                    }
                }
            },
            0,
            Integer.parseInt(humanMachineProperties.getProperty("agent.personnel.interval")),
            TimeUnit.SECONDS);

    }

    @Override
    public void stop() {
        super.stop();
        scheduler.shutdown();
    }
}
