package org.datacenter.agent.personnel;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.datacenter.agent.BaseAgent;
import org.datacenter.agent.util.KafkaUtil;
import org.datacenter.agent.util.PersonnelAndFlightPlanHttpClientUtil;
import org.datacenter.config.HumanMachineConfig;
import org.datacenter.config.receiver.PersonnelAndPlanLoginConfig;
import org.datacenter.config.receiver.crew.PersonnelAgentReceiverConfig;
import org.datacenter.exception.ZorathosException;
import org.datacenter.model.crew.PersonnelInfo;
import org.datacenter.receiver.util.DataReceiverUtil;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.datacenter.config.keys.HumanMachineReceiverConfigKey.PERSONNEL_AND_PLAN_LOGIN_JSON;
import static org.datacenter.config.keys.HumanMachineReceiverConfigKey.PERSONNEL_AND_PLAN_LOGIN_URL;
import static org.datacenter.config.keys.HumanMachineReceiverConfigKey.PERSONNEL_OUTPUT_DIRECTORY;
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

    private final ObjectMapper mapper = DataReceiverUtil.mapper;
    private final PersonnelAgentReceiverConfig receiverConfig;
    private ScheduledExecutorService scheduler;
    private PersonnelAndPlanLoginConfig loginConfig;

    public PersonnelAgent(PersonnelAndPlanLoginConfig loginConfig, PersonnelAgentReceiverConfig receiverConfig) {
        super();
        this.loginConfig = loginConfig;
        this.receiverConfig = receiverConfig;
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
                            if (personnelInfos.isEmpty()) {
                                log.info("Fetched nothing from personnel agent system.");
                                return;
                            }
                            if (receiverConfig.getStartupMode().equals(PersonnelAgentReceiverConfig.StartupMode.KAFKA)) {
                                sendPersonnelInfosToKafka(personnelInfos);
                            } else if (receiverConfig.getStartupMode().equals(PersonnelAgentReceiverConfig.StartupMode.JSON_FILE)) {
                                savePersonnelInfosToFile(personnelInfos);
                            }
                        }
                    } catch (Exception e) {
                        log.error("Error caught by scheduler pool. Task will be stopped.");
                        stop();
                    }
                },
                0, Integer.parseInt(HumanMachineConfig.getProperty(AGENT_INTERVAL_PERSONNEL)), TimeUnit.MINUTES);
    }

    private void savePersonnelInfosToFile(List<PersonnelInfo> personnelInfos) throws IOException {
        // 存储到JSON文件
        File logDir = new File(receiverConfig.getOutputDir());
        if (!logDir.exists()) {
            boolean mkdirs = logDir.mkdirs();
            if (!mkdirs) {
                log.error("Can't create directory: {}", logDir.getAbsolutePath());
                throw new ZorathosException("Can't create directory: " + logDir.getAbsolutePath());
            }
        } else {
            log.info("Log dir for personnel agent already exists: {}", logDir.getAbsolutePath());
        }
        // 以personnel_timestamp.json的格式存储
        File jsonFile = new File(logDir, "personnel_" + System.currentTimeMillis() + ".json");
        Files.writeString(jsonFile.toPath(),
                mapper.writerWithDefaultPrettyPrinter().writeValueAsString(personnelInfos),
                StandardCharsets.UTF_8);
    }

    private void sendPersonnelInfosToKafka(List<PersonnelInfo> personnelInfos) {
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
     * 离线拉去数据自启动时的主函数入口
     * @param args 输入参数
     */
    public static void main(String[] args) {
        ParameterTool params = ParameterTool.fromArgs(args);
        log.info("Parameters: {}", params.toMap());

        String encodedLoginJson = params.getRequired(PERSONNEL_AND_PLAN_LOGIN_JSON.getKeyForParamsMap());
        String decodedLoginJson = new String(Base64.getDecoder().decode(encodedLoginJson));

        PersonnelAndPlanLoginConfig loginConfig = PersonnelAndPlanLoginConfig.builder()
                .loginUrl(params.getRequired(PERSONNEL_AND_PLAN_LOGIN_URL.getKeyForParamsMap()))
                .loginJson(decodedLoginJson)
                .build();
        PersonnelAgentReceiverConfig receiverConfig = PersonnelAgentReceiverConfig.builder()
                .url(params.getRequired(PERSONNEL_AND_PLAN_LOGIN_URL.getKeyForParamsMap()))
                .startupMode(PersonnelAgentReceiverConfig.StartupMode.JSON_FILE)
                .outputDir(params.getRequired(PERSONNEL_OUTPUT_DIRECTORY.getKeyForParamsMap()))
                .build();
        PersonnelAgent personnelAgent = new PersonnelAgent(loginConfig, receiverConfig);

        // shutdownhook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown hook triggered. Stopping Personnel agent.");
            personnelAgent.stop();
        }));

        Thread agentThread = new Thread(personnelAgent);
        agentThread.setUncaughtExceptionHandler((thread, throwable) -> {
            log.error("Personnel agent thread {} encountered an error: {}", thread.getName(), throwable.getMessage());
            // 这里可以添加一些清理操作，比如关闭连接等
            personnelAgent.stop();
        });
        agentThread.start();
    }
}
