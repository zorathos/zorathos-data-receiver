package org.datacenter.agent.plan;

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
import org.datacenter.config.receiver.plan.FlightPlanAgentReceiverConfig;
import org.datacenter.exception.ZorathosException;
import org.datacenter.model.base.TiDBDatabase;
import org.datacenter.model.plan.FlightPlanRoot;
import org.datacenter.receiver.util.DataReceiverUtil;
import org.datacenter.receiver.util.MySQLDriverConnectionPool;

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

import static org.datacenter.config.keys.HumanMachineReceiverConfigKey.FLIGHT_PLAN_FLIGHT_CODE_URL;
import static org.datacenter.config.keys.HumanMachineReceiverConfigKey.FLIGHT_PLAN_FLIGHT_XML_URL;
import static org.datacenter.config.keys.HumanMachineReceiverConfigKey.PERSONNEL_AND_PLAN_LOGIN_JSON;
import static org.datacenter.config.keys.HumanMachineReceiverConfigKey.PERSONNEL_AND_PLAN_LOGIN_URL;
import static org.datacenter.config.keys.HumanMachineReceiverConfigKey.PERSONNEL_OUTPUT_DIRECTORY;
import static org.datacenter.config.keys.HumanMachineSysConfigKey.AGENT_INTERVAL_FLIGHT_PLAN;
import static org.datacenter.config.keys.HumanMachineSysConfigKey.KAFKA_TOPIC_FLIGHT_PLAN_ROOT;

/**
 * @author : [wangminan]
 * @description : 通过飞行计划接口获取飞行计划数据 投递到Kafka
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Slf4j
@AllArgsConstructor
public class FlightPlanAgent extends BaseAgent {

    private final ObjectMapper mapper = DataReceiverUtil.mapper;
    private ScheduledExecutorService scheduler;
    private PersonnelAndPlanLoginConfig loginConfig;
    private FlightPlanAgentReceiverConfig receiverConfig;
    private MySQLDriverConnectionPool tidbFlightPlanPool;

    public FlightPlanAgent(PersonnelAndPlanLoginConfig loginConfig,
                           FlightPlanAgentReceiverConfig receiverConfig) {
        super();
        this.loginConfig = loginConfig;
        this.receiverConfig = receiverConfig;
        this.tidbFlightPlanPool = new MySQLDriverConnectionPool(TiDBDatabase.FLIGHT_PLAN);
    }

    @Override
    public void run() {
        super.run();
        if (!isStartedByThisInstance) {
            return;
        }

        log.info("Flight plan agent start running, fetching data from flight agent system's xml interface and sending it to kafka.");

        if (scheduler == null) {
            scheduler = Executors.newScheduledThreadPool(1, r -> {
                Thread t = new Thread(r);
                t.setName("FlightPlanAgent");
                return t;
            });
        }

        scheduler.scheduleAtFixedRate(() -> {
            try {
                if (prepared) {
                    // 0. 刷新Cookie
                    PersonnelAndFlightPlanHttpClientUtil.loginAndGetCookies(loginConfig);

                    // 1. 准备 Kafka 的 consumer group并创建所有 topic
                    KafkaUtil.createTopicIfNotExists(HumanMachineConfig.getProperty(KAFKA_TOPIC_FLIGHT_PLAN_ROOT));
                    running = true;
                    log.info("Flight plan agent is running.");

                    // 2. 获取飞行计划根XML并解析
                    List<FlightPlanRoot> flightPlans = PersonnelAndFlightPlanHttpClientUtil.getFlightRoots(receiverConfig, tidbFlightPlanPool);
                    // 所有日期都已导入完成
                    if (flightPlans.isEmpty()) {
                        log.info("All flight plans have been imported.");
                        return;
                    }
                    if (receiverConfig.getStartupMode().equals(FlightPlanAgentReceiverConfig.StartupMode.KAFKA)) {
                        sendFlightPlansToKafka(flightPlans);
                    } else if (receiverConfig.getStartupMode().equals(FlightPlanAgentReceiverConfig.StartupMode.JSON_FILE)) {
                        saveFlightPlanToFile(flightPlans);
                    }
                }
            } catch (Exception e) {
                log.error("Error caught by scheduler pool. Task will be stopped.");
                stop();
            }
        }, 0, Integer.parseInt(HumanMachineConfig.getProperty(AGENT_INTERVAL_FLIGHT_PLAN)), TimeUnit.MINUTES);
    }

    private void saveFlightPlanToFile(List<FlightPlanRoot> flightPlanRoots) throws IOException {
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
        // 以flight_plan_timestamp.json的格式存储
        File jsonFile = new File(logDir, "flight_plan_" + System.currentTimeMillis() + ".json");
        Files.writeString(jsonFile.toPath(),
                mapper.writerWithDefaultPrettyPrinter().writeValueAsString(flightPlanRoots),
                StandardCharsets.UTF_8);
    }

    private void sendFlightPlansToKafka(List<FlightPlanRoot> flightPlans) {
        // 2. 转发到Kafka
        List<CompletableFuture<Void>> futures = flightPlans.stream()
                .map(flightPlan -> CompletableFuture.runAsync(() -> {
                    try {
                        String flightPlanInJson = mapper.writeValueAsString(flightPlan);
                        KafkaUtil.sendMessage(HumanMachineConfig.getProperty(KAFKA_TOPIC_FLIGHT_PLAN_ROOT), flightPlanInJson);
                    } catch (JsonProcessingException e) {
                        throw new ZorathosException(e, "Error occurred while converting flight plan to json.");
                    }
                }))
                .toList();
        CompletableFuture<Void> allFutures = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
        try {
            allFutures.join();
        } catch (CompletionException e) {
            throw new ZorathosException(e.getCause(), "Error in parallel processing of flight plan.");
        }
    }

    @Override
    public void stop() {
        super.stop();
        try {
            scheduler.shutdown();
        } catch (Exception ex) {
            log.error("Error shutting down scheduler", ex);
        }
        tidbFlightPlanPool.closePool();
        System.exit(0);
    }

    public static void main(String[] args) {
        ParameterTool params = ParameterTool.fromArgs(args);
        log.info("Parameters: {}", params.toMap());

        String encodedLoginJson = params.getRequired(PERSONNEL_AND_PLAN_LOGIN_JSON.getKeyForParamsMap());
        String decodedLoginJson = new String(Base64.getDecoder().decode(encodedLoginJson));

        // 1. 加载配置
        PersonnelAndPlanLoginConfig loginConfig = PersonnelAndPlanLoginConfig.builder()
                .loginUrl(params.getRequired(PERSONNEL_AND_PLAN_LOGIN_URL.getKeyForParamsMap()))
                .loginJson(decodedLoginJson)
                .build();
        FlightPlanAgentReceiverConfig receiverConfig = FlightPlanAgentReceiverConfig.builder()
                .flightDateUrl(params.getRequired(FLIGHT_PLAN_FLIGHT_CODE_URL.getKeyForParamsMap()))
                .flightXmlUrl(params.getRequired(FLIGHT_PLAN_FLIGHT_XML_URL.getKeyForParamsMap()))
                .startupMode(FlightPlanAgentReceiverConfig.StartupMode.JSON_FILE)
                .outputDir(params.getRequired(PERSONNEL_OUTPUT_DIRECTORY.getKeyForParamsMap()))
                .build();

        FlightPlanAgent flightPlanAgent = new FlightPlanAgent(loginConfig, receiverConfig);
        // shutdownhook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown hook triggered. Stopping Flight plan agent.");
            flightPlanAgent.stop();
        }));
        Thread agentThread = new Thread(flightPlanAgent);
        agentThread.setUncaughtExceptionHandler((thread, throwable) -> {
            log.error("Flight plan agent thread {} encountered an error: {}", thread.getName(), throwable.getMessage());
            // 这里可以添加一些清理操作，比如关闭连接等
            flightPlanAgent.stop();
        });
        agentThread.start();
    }
}
