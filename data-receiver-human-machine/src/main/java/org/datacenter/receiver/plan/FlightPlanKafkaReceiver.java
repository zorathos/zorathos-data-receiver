package org.datacenter.receiver.plan;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.datacenter.agent.plan.FlightPlanAgent;
import org.datacenter.config.HumanMachineConfig;
import org.datacenter.config.receiver.PersonnelAndPlanLoginConfig;
import org.datacenter.config.receiver.plan.FlightPlanAgentReceiverConfig;
import org.datacenter.exception.ZorathosException;
import org.datacenter.model.base.TiDBDatabase;
import org.datacenter.model.plan.FlightPlanRoot;
import org.datacenter.receiver.BaseReceiver;
import org.datacenter.receiver.plan.util.FlightPlanSinkUtil;
import org.datacenter.receiver.util.DataReceiverUtil;

import java.util.Base64;
import java.util.List;

import static org.datacenter.config.keys.HumanMachineReceiverConfigKey.FLIGHT_PLAN_FLIGHT_CODE_URL;
import static org.datacenter.config.keys.HumanMachineReceiverConfigKey.FLIGHT_PLAN_FLIGHT_XML_URL;
import static org.datacenter.config.keys.HumanMachineReceiverConfigKey.PERSONNEL_AND_PLAN_LOGIN_JSON;
import static org.datacenter.config.keys.HumanMachineReceiverConfigKey.PERSONNEL_AND_PLAN_LOGIN_URL;
import static org.datacenter.config.keys.HumanMachineSysConfigKey.KAFKA_TOPIC_FLIGHT_PLAN_ROOT;

/**
 * @author : [wangminan]
 * @description : 从Kafka中接收飞行计划数据写入TiDB
 */
@Slf4j
public class FlightPlanKafkaReceiver extends BaseReceiver {

    private final FlightPlanAgent flightPlanAgent;

    public FlightPlanKafkaReceiver(PersonnelAndPlanLoginConfig loginConfig,
                                   FlightPlanAgentReceiverConfig FlightPlanAgentReceiverConfig) {
        // 1. 加载配置 HumanMachineConfig.loadConfig();
        HumanMachineConfig sysConfig = new HumanMachineConfig();
        sysConfig.loadConfig();
        this.flightPlanAgent = new FlightPlanAgent(loginConfig, FlightPlanAgentReceiverConfig);
    }

    @Override
    public void prepare() {
        super.prepare();
        Thread agentThread = new Thread(flightPlanAgent);
        agentThread.setUncaughtExceptionHandler((thread, throwable) -> {
            log.error("Flight plan agent thread {} encountered an error: {}", thread.getName(), throwable.getMessage());
            agentShutdown(flightPlanAgent);
        });
        agentThread.start();
        awaitAgentRunning(flightPlanAgent);
    }

    @Override
    public void start() {
        // shutdownhook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> agentShutdown(flightPlanAgent)));

        // 开始从kafka获取数据
        // 引入执行环境
        StreamExecutionEnvironment env = DataReceiverUtil.prepareStreamEnv();

        SingleOutputStreamOperator<FlightPlanRoot> kafkaSourceDS = DataReceiverUtil
                .getKafkaSourceDS(env, List.of(HumanMachineConfig.getProperty(KAFKA_TOPIC_FLIGHT_PLAN_ROOT)), FlightPlanRoot.class)
                .returns(FlightPlanRoot.class);

        // 重复使用datastream flink在每一次对datastream操作之后都会new一个新的对象 所以不用担心反复消费的问题
        FlightPlanSinkUtil.addMultiSinkForFlightPlanRoot(kafkaSourceDS, TiDBDatabase.FLIGHT_PLAN, null);

        try {
            env.execute();
        } catch (Exception e) {
            throw new ZorathosException(e, "Encounter error when sinking flight plan data to tidb.");
        }
    }

    /**
     * 主函数
     *
     * @param args 输入参数
     *             --loginUrl http://xxxx --loginJson eyxxx== --flightDateUrl http://xxxx
     *             --flightCodeUrl http://xxxx --flightXmlUrl http://xxxx
     */
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
                .build();

        FlightPlanKafkaReceiver receiver = new FlightPlanKafkaReceiver(loginConfig, receiverConfig);
        receiver.run();
    }
}
