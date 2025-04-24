package org.datacenter.receiver.crew;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.datacenter.agent.personnel.PersonnelAgent;
import org.datacenter.config.HumanMachineConfig;
import org.datacenter.config.receiver.PersonnelAndPlanLoginConfig;
import org.datacenter.config.receiver.crew.PersonnelAgentReceiverConfig;
import org.datacenter.exception.ZorathosException;
import org.datacenter.model.crew.PersonnelInfo;
import org.datacenter.receiver.BaseReceiver;
import org.datacenter.receiver.crew.util.PersonnelSinkUtil;
import org.datacenter.receiver.util.DataReceiverUtil;

import java.util.Base64;
import java.util.List;

import static org.datacenter.config.keys.HumanMachineReceiverConfigKey.PERSONNEL_AND_PLAN_LOGIN_JSON;
import static org.datacenter.config.keys.HumanMachineReceiverConfigKey.PERSONNEL_AND_PLAN_LOGIN_URL;
import static org.datacenter.config.keys.HumanMachineSysConfigKey.KAFKA_TOPIC_PERSONNEL;

/**
 * @author : [wangminan]
 * @description : 人员数据Kafka接收器
 */
@Slf4j
public class PersonnelKafkaReceiver extends BaseReceiver {

    private final PersonnelAgent personnelAgent;

    public PersonnelKafkaReceiver(PersonnelAndPlanLoginConfig loginConfig, PersonnelAgentReceiverConfig receiverConfig) {
        // 1. 加载配置 HumanMachineConfig.loadConfig();
        HumanMachineConfig sysConfig = new HumanMachineConfig();
        sysConfig.loadConfig();
        // 2. 初始化人员Agent
        this.personnelAgent = new PersonnelAgent(loginConfig, receiverConfig);
    }

    @Override
    public void prepare() {
        super.prepare();
        Thread agentThread = new Thread(personnelAgent);
        agentThread.setUncaughtExceptionHandler((thread, throwable) -> {
            log.error("Personnel agent thread {} encountered an error: {}", thread.getName(), throwable.getMessage());
            // 这里可以添加一些清理操作，比如关闭连接等
            agentShutdown(personnelAgent);
        });
        agentThread.start();
        awaitAgentRunning(personnelAgent);
    }

    @Override
    public void start() {
        // shutdownhook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> agentShutdown(personnelAgent)));

        // 开始从kafka获取数据
        // 引入执行环境
        StreamExecutionEnvironment env = DataReceiverUtil.prepareStreamEnv();
        DataStreamSource<PersonnelInfo> kafkaSourceDS =
                DataReceiverUtil.getKafkaSourceDS(env, List.of(HumanMachineConfig.getProperty(KAFKA_TOPIC_PERSONNEL)), PersonnelInfo.class);

        kafkaSourceDS.sinkTo(PersonnelSinkUtil.personnelJdbcSink).name("Personnel kafka sink.");
        try {
            env.execute();
        } catch (Exception e) {
            throw new ZorathosException(e, "Encounter error when sinking personnel data to tidb.");
        }
    }

    /**
     * 被flink调用的主函数
     * 参数输入形式为 --loginUrl xxx --loginJson xxx --personnelUrl xxx
     *
     * @param args 参数 第一个为接收器参数 第二个为持久化器参数
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
                .startupMode(PersonnelAgentReceiverConfig.StartupMode.KAFKA)
                .build();
        PersonnelKafkaReceiver personnelKafkaReceiver = new PersonnelKafkaReceiver(loginConfig, receiverConfig);
        personnelKafkaReceiver.run();
    }
}
