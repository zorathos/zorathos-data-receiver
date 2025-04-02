package org.datacenter.agent.personnel;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.datacenter.agent.BaseAgent;
import org.datacenter.agent.util.KafkaUtil;
import org.datacenter.agent.util.PersonnelAndFlightPlanHttpClientUtil;
import org.datacenter.config.PersonnelAndPlanLoginConfig;
import org.datacenter.config.crew.PersonnelReceiverConfig;
import org.datacenter.exception.ZorathosException;
import org.datacenter.model.crew.PersonnelInfo;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.datacenter.config.system.BaseSysConfig.humanMachineProperties;

/**
 * @author : [wangminan]
 * @description : 人员信息拉取Agent
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Slf4j
@AllArgsConstructor
public class PersonnelAgent extends BaseAgent {

    private final ObjectMapper mapper;
    private final PersonnelReceiverConfig receiverConfig;
    private ScheduledExecutorService scheduler;
    private PersonnelAndPlanLoginConfig loginConfig;

    public PersonnelAgent(PersonnelAndPlanLoginConfig loginConfig, PersonnelReceiverConfig receiverConfig) {
        super();
        this.loginConfig = loginConfig;
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

        log.info("Personnel agent start running, fetching data from personnel agent system's json interface and sending it to kafka.");

        if (scheduler == null) {
            scheduler = Executors.newScheduledThreadPool(1);
        }

        scheduler.scheduleAtFixedRate(() -> {
            if (prepared) {
                // 这玩意没有主键 所以在每一次写入之前都需要清空所有原有数据
                // 0. 刷新Cookie
                PersonnelAndFlightPlanHttpClientUtil.loginAndGetCookies(loginConfig);
                // 1 准备 Kafka 的 consumer group并创建所有 topic
                KafkaUtil.createTopicIfNotExists(humanMachineProperties.getProperty("kafka.topic.personnel"));
                // 这时候才可以拉起Flink任务
                running = true;
                log.info("Personnel agent is running.");
                // 2. 拉取人员数据
                List<PersonnelInfo> personnelInfos = PersonnelAndFlightPlanHttpClientUtil.getPersonnelInfos(receiverConfig);
                // 3. 转发到Kafka
                for (PersonnelInfo personnelInfo : personnelInfos) {
                    try {
                        String personnelInfoInJson = mapper.writeValueAsString(personnelInfo);
                        KafkaUtil.sendMessage(humanMachineProperties
                                .getProperty("kafka.topic.personnel"), personnelInfoInJson);
                    } catch (JsonProcessingException e) {
                        throw new ZorathosException(e, "Error occurs while converting personnel infos to json string.");
                    }
                }
            }
        },
        0, Integer.parseInt(humanMachineProperties.getProperty("agent.interval.personnel")), TimeUnit.MINUTES);
    }

    @Override
    public void stop() {
        super.stop();
        scheduler.shutdown();
    }
}
