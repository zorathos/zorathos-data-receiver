package org.datacenter.agent.plan;

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
import org.datacenter.config.agent.plan.FlightPlanAgentConfig;
import org.datacenter.exception.ZorathosException;
import org.datacenter.model.base.TiDBDatabase;
import org.datacenter.model.plan.FlightPlanRoot;
import org.datacenter.util.MySQLDriverConnectionPool;

import java.util.Base64;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.datacenter.config.keys.HumanMachineAgentConfigKey.FLIGHT_PLAN_FLIGHT_CODE_URL;
import static org.datacenter.config.keys.HumanMachineAgentConfigKey.FLIGHT_PLAN_FLIGHT_DATE_URL;
import static org.datacenter.config.keys.HumanMachineAgentConfigKey.FLIGHT_PLAN_FLIGHT_XML_URL;
import static org.datacenter.config.keys.HumanMachineAgentConfigKey.PERSONNEL_AND_PLAN_LOGIN_JSON;
import static org.datacenter.config.keys.HumanMachineAgentConfigKey.PERSONNEL_AND_PLAN_LOGIN_URL;
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

    private final ObjectMapper mapper;
    private ScheduledExecutorService scheduler;
    private PersonnelAndPlanLoginConfig loginConfig;
    private FlightPlanAgentConfig FlightPlanAgentConfig;
    private MySQLDriverConnectionPool tidbFlightPlanPool;

    public FlightPlanAgent(PersonnelAndPlanLoginConfig loginConfig,
                           FlightPlanAgentConfig FlightPlanAgentConfig) {
        super();
        this.mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        this.loginConfig = loginConfig;
        this.FlightPlanAgentConfig = FlightPlanAgentConfig;
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
                    List<FlightPlanRoot> flightPlans = PersonnelAndFlightPlanHttpClientUtil.getFlightRoots(FlightPlanAgentConfig, tidbFlightPlanPool);
                    // 所有日期都已导入完成
                    if (flightPlans.isEmpty()) {
                        log.info("All flight plans have been imported.");
                        return;
                    }
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
            } catch (Exception e) {
                log.error("Error caught by scheduler pool. Task will be stopped.");
                stop();
            }
        }, 0, Integer.parseInt(HumanMachineConfig.getProperty(AGENT_INTERVAL_FLIGHT_PLAN)), TimeUnit.MINUTES);
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

    /**
     * 主函数
     *
     * @param args 输入参数
     *             --loginUrl http://xxxx --loginJson eyxxx== --flightDateUrl http://xxxx
     *             --flightCodeUrl http://xxxx --flightXmlUrl http://xxxx
     */
    public static void main(String[] args) {
        log.info("Flight Plan Agent starting...");
        ParameterUtil params = ParameterUtil.fromArgs(args);
        log.info("Parameters: {}", params);

        String encodedLoginJson = params.getRequired(PERSONNEL_AND_PLAN_LOGIN_JSON.getKeyForParamsMap());
        String decodedLoginJson = new String(Base64.getDecoder().decode(encodedLoginJson));

        // 1. 加载配置
        PersonnelAndPlanLoginConfig loginConfig = PersonnelAndPlanLoginConfig.builder()
                .loginUrl(params.getRequired(PERSONNEL_AND_PLAN_LOGIN_URL.getKeyForParamsMap()))
                .loginJson(decodedLoginJson)
                .build();
        FlightPlanAgentConfig flightPlanAgentConfig = org.datacenter.config.agent.plan.FlightPlanAgentConfig.builder()
                .flightDateUrl(params.getRequired(FLIGHT_PLAN_FLIGHT_DATE_URL.getKeyForParamsMap()))
                .flightCodeUrl(params.getRequired(FLIGHT_PLAN_FLIGHT_CODE_URL.getKeyForParamsMap()))
                .flightXmlUrl(params.getRequired(FLIGHT_PLAN_FLIGHT_XML_URL.getKeyForParamsMap()))
                .build();

        FlightPlanAgent flightPlanAgent = new FlightPlanAgent(loginConfig, flightPlanAgentConfig);
        try (ExecutorService executorService = Executors.newSingleThreadExecutor()) {
            executorService.submit(flightPlanAgent);
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                flightPlanAgent.stop();
                log.info("Flight plan agent stopped.");
            }));
        } catch (Exception e) {
            log.error("Error starting flight plan agent", e);
            flightPlanAgent.stop();
        }
    }
}
