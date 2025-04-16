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
import org.datacenter.agent.util.ParameterUtil;
import org.datacenter.agent.util.PersonnelAndFlightPlanHttpClientUtil;
import org.datacenter.config.HumanMachineConfig;
import org.datacenter.config.agent.PersonnelAndPlanLoginConfig;
import org.datacenter.config.agent.crew.PersonnelAgentConfig;
import org.datacenter.exception.ZorathosException;
import org.datacenter.model.crew.PersonnelInfo;

import java.util.Base64;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.datacenter.config.keys.HumanMachineAgentConfigKey.PERSONNEL_AND_PLAN_LOGIN_JSON;
import static org.datacenter.config.keys.HumanMachineAgentConfigKey.PERSONNEL_AND_PLAN_LOGIN_URL;
import static org.datacenter.config.keys.HumanMachineAgentConfigKey.PERSONNEL_URL;
import static org.datacenter.config.keys.HumanMachineSysConfigKey.AGENT_INTERVAL_PERSONNEL;
import static org.datacenter.config.keys.HumanMachineSysConfigKey.KAFKA_TOPIC_PERSONNEL;

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
    private final PersonnelAgentConfig receiverConfig;
    private ScheduledExecutorService scheduler;
    private PersonnelAndPlanLoginConfig loginConfig;

    public PersonnelAgent(PersonnelAndPlanLoginConfig loginConfig, PersonnelAgentConfig receiverConfig) {
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
            scheduler = Executors.newScheduledThreadPool(1, r -> {
                Thread t = new Thread(r);
                t.setName("PersonnelAgent-Scheduler");
                return t;
            });
        }

        scheduler.scheduleAtFixedRate(() -> {
                    try {
                        if (prepared) {
                            // 这玩意没有主键 所以在每一次写入之前都需要清空所有原有数据
                            // 0. 刷新Cookie
                            PersonnelAndFlightPlanHttpClientUtil.loginAndGetCookies(loginConfig);
                            // 1 准备 Kafka 的 consumer group并创建所有 topic
                            KafkaUtil.createTopicIfNotExists(HumanMachineConfig.getProperty(KAFKA_TOPIC_PERSONNEL));
                            // 这时候才可以拉起Flink任务
                            running = true;
                            log.info("Personnel agent is running.");
                            // 2. 拉取人员数据
                            List<PersonnelInfo> personnelInfos = PersonnelAndFlightPlanHttpClientUtil.getPersonnelInfos(receiverConfig);
                            // 3. 转发到Kafka
                            List<CompletableFuture<Void>> futures = personnelInfos.stream()
                                    .map(personnelInfo -> CompletableFuture.runAsync(() -> {
                                        try {
                                            String personnelInfoInJson = mapper.writeValueAsString(personnelInfo);
                                            KafkaUtil.sendMessage(HumanMachineConfig.getProperty(KAFKA_TOPIC_PERSONNEL), personnelInfoInJson);
                                        } catch (JsonProcessingException e) {
                                            throw new ZorathosException(e, "Error occurred while converting personnel info to json.");
                                        }
                                    }))
                                    .toList();
                            CompletableFuture<Void> allFutures = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
                            try {
                                allFutures.join();
                            } catch (CompletionException e) {
                                throw new ZorathosException(e.getCause(), "Error in parallel processing of personnel info.");
                            }
                        }
                    } catch (Exception e) {
                        log.error("Error caught by scheduler pool. Task will be stopped.");
                        stop();
                    }
                },
                0, Integer.parseInt(HumanMachineConfig.getProperty(AGENT_INTERVAL_PERSONNEL)), TimeUnit.MINUTES);
    }

    @Override
    public void stop() {
        super.stop();
        try {
            scheduler.shutdownNow();
        } catch (Exception ex) {
            log.error("Error shutting down scheduler", ex);
        }
        System.exit(0);
    }

    /**
     * 被flink调用的主函数
     * 参数输入形式为 --loginUrl xxx --loginJson xxx --personnelUrl xxx
     *
     * @param args 参数 第一个为接收器参数 第二个为持久化器参数
     */
    public static void main(String[] args) {
        log.info("Personnel agent starting.");
        ParameterUtil params = ParameterUtil.fromArgs(args);
        log.info("Parameters: {}", params.toMap());

        String encodedLoginJson = params.getRequired(PERSONNEL_AND_PLAN_LOGIN_JSON.getKeyForParamsMap());
        String decodedLoginJson = new String(Base64.getDecoder().decode(encodedLoginJson));

        PersonnelAndPlanLoginConfig loginConfig = PersonnelAndPlanLoginConfig.builder()
                .loginUrl(params.getRequired(PERSONNEL_AND_PLAN_LOGIN_URL.getKeyForParamsMap()))
                .loginJson(decodedLoginJson)
                .build();
        PersonnelAgentConfig receiverConfig = PersonnelAgentConfig.builder()
                .url(params.getRequired(PERSONNEL_URL.getKeyForParamsMap()))
                .build();

        PersonnelAgent personnelAgent = new PersonnelAgent(loginConfig, receiverConfig);
        try (ExecutorService executorService = Executors.newSingleThreadExecutor()) {
            executorService.submit(personnelAgent);
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("Shutting down Personnel Agent...");
                personnelAgent.stop();
            }));
        } catch (Exception e) {
            log.error("Error starting Personnel Agent", e);
            throw new ZorathosException(e, "Error starting Personnel Agent");
        }
    }
}
