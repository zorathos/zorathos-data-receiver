package org.datacenter.receiver.sorties;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.jdbc.sink.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.datacenter.agent.sorties.SortiesAgent;
import org.datacenter.config.HumanMachineConfig;
import org.datacenter.config.receiver.sorties.SortiesBatchReceiverConfig;
import org.datacenter.config.receiver.sorties.SortiesReceiverConfig;
import org.datacenter.exception.ZorathosException;
import org.datacenter.model.base.TiDBDatabase;
import org.datacenter.model.sorties.Sorties;
import org.datacenter.receiver.BaseReceiver;
import org.datacenter.receiver.util.DataReceiverUtil;
import org.datacenter.receiver.util.JdbcSinkUtil;

import java.sql.Timestamp;
import java.util.Base64;
import java.util.List;

import static org.datacenter.config.keys.HumanMachineReceiverConfigKey.SORTIES_BASE_URL;
import static org.datacenter.config.keys.HumanMachineReceiverConfigKey.SORTIES_BATCH_JSON;
import static org.datacenter.config.keys.HumanMachineReceiverConfigKey.SORTIES_BATCH_URL;
import static org.datacenter.config.keys.HumanMachineSysConfigKey.KAFKA_TOPIC_SORTIES;

/**
 * @author : [wangminan]
 * @description : 架次信息用的KafkaReceiver
 */
@Slf4j
public class SortiesKafkaReceiver extends BaseReceiver {

    private final SortiesAgent sortiesAgent;

    public SortiesKafkaReceiver(SortiesBatchReceiverConfig batchReceiverConfig, SortiesReceiverConfig sortiesReceiverConfig) {
        // 1. 加载配置 HumanMachineConfig.loadConfig();
        HumanMachineConfig sysConfig = new HumanMachineConfig();
        sysConfig.loadConfig();
        this.sortiesAgent = new SortiesAgent(batchReceiverConfig, sortiesReceiverConfig);
    }

    @Override
    public void prepare() {
        super.prepare();
        Thread agentThread = new Thread(sortiesAgent);
        agentThread.setUncaughtExceptionHandler((thread, throwable) -> {
            log.error("Sorties agent thread {} encountered an error: {}", thread.getName(), throwable.getMessage());
            agentShutdown(sortiesAgent);
        });
        agentThread.start();
        awaitAgentRunning(sortiesAgent);
    }

    @Override
    public void start() {
        // 1. shutdownhook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> agentShutdown(sortiesAgent)));

        // 2. 执行环境 引入kafka数据源
        StreamExecutionEnvironment env = DataReceiverUtil.prepareStreamEnv();
        DataStreamSource<Sorties> kafkaSourceDS =
                DataReceiverUtil.getKafkaSourceDS(env, List.of(HumanMachineConfig.getProperty(KAFKA_TOPIC_SORTIES)), Sorties.class);

        // 3. jdbcsink
        Sink<Sorties> sink = JdbcSink.<Sorties>builder()
                .withQueryStatement("""
                        INSERT INTO sorties (
                            airplane_model, airplane_number, arm_type, batch_number, camp, camp_str, car_end_time, car_start_time, create_time, down_pilot, folder_id, folder_name, icd_version, interpolation, is_effective, is_effective_name, location, pilot, qx_id, remark, role, role_str, sky_time, sortie_id, sortie_number, source, stealth, stealth_str, subject, sync_system, sync_system_str, test_drive, test_drive_str, up_pilot
                        ) VALUES (
                            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                        ) ON DUPLICATE KEY UPDATE
                            airplane_model = VALUES(airplane_model),
                            airplane_number = VALUES(airplane_number),
                            arm_type = VALUES(arm_type),
                            batch_number = VALUES(batch_number),
                            camp = VALUES(camp),
                            camp_str = VALUES(camp_str),
                            car_end_time = VALUES(car_end_time),
                            car_start_time = VALUES(car_start_time),
                            create_time = VALUES(create_time),
                            down_pilot = VALUES(down_pilot),
                            folder_id = VALUES(folder_id),
                            folder_name = VALUES(folder_name),
                            icd_version = VALUES(icd_version),
                            interpolation = VALUES(interpolation),
                            is_effective = VALUES(is_effective),
                            is_effective_name = VALUES(is_effective_name),
                            location = VALUES(location),
                            pilot = VALUES(pilot),
                            qx_id = VALUES(qx_id),
                            remark = VALUES(remark),
                            role = VALUES(role),
                            role_str = VALUES(role_str),
                            sky_time = VALUES(sky_time),
                            sortie_number = VALUES(sortie_number),
                            source = VALUES(source),
                            stealth = VALUES(stealth),
                            stealth_str = VALUES(stealth_str),
                            subject = VALUES(subject),
                            sync_system = VALUES(sync_system),
                            sync_system_str = VALUES(sync_system_str),
                            test_drive = VALUES(test_drive),
                            test_drive_str = VALUES(test_drive_str),
                            up_pilot = VALUES(up_pilot);
                        """, (JdbcStatementBuilder<Sorties>) (preparedStatement, sorties) -> {
                    preparedStatement.setString(1, sorties.getAirplaneModel());
                    preparedStatement.setString(2, sorties.getAirplaneNumber());
                    preparedStatement.setString(3, sorties.getArmType());
                    preparedStatement.setString(4, sorties.getBatchNumber());
                    preparedStatement.setLong(5, sorties.getCamp());
                    preparedStatement.setString(6, sorties.getCampStr());
                    preparedStatement.setTimestamp(7,
                            sorties.getCarEndTime() == null ? null :
                                    Timestamp.valueOf(sorties.getCarEndTime()));
                    preparedStatement.setTimestamp(8,
                            sorties.getCarStartTime() == null ? null :
                                    Timestamp.valueOf(sorties.getCarStartTime()));
                    preparedStatement.setTimestamp(9,
                            sorties.getCreateTime() == null ? null :
                                    Timestamp.valueOf(sorties.getCreateTime()));
                    preparedStatement.setString(10, sorties.getDownPilot());
                    preparedStatement.setString(11, sorties.getFolderId());
                    preparedStatement.setString(12, sorties.getFolderName());
                    preparedStatement.setString(13, sorties.getIcdVersion());
                    preparedStatement.setLong(14, sorties.getInterpolation());
                    preparedStatement.setLong(15, sorties.getIsEffective());
                    preparedStatement.setString(16, sorties.getIsEffectiveName());
                    preparedStatement.setString(17, sorties.getLocation());
                    preparedStatement.setString(18, sorties.getPilot());
                    preparedStatement.setString(19, sorties.getQxId());
                    preparedStatement.setString(20, sorties.getRemark());
                    preparedStatement.setLong(21, sorties.getRole());
                    preparedStatement.setString(22, sorties.getRoleStr());
                    preparedStatement.setString(23, sorties.getSkyTime());
                    preparedStatement.setString(24, sorties.getSortieId());
                    preparedStatement.setString(25, sorties.getSortieNumber());
                    preparedStatement.setLong(26, sorties.getSource());
                    preparedStatement.setLong(27, sorties.getStealth());
                    preparedStatement.setString(28, sorties.getStealthStr());
                    preparedStatement.setString(29, sorties.getSubject());
                    preparedStatement.setLong(30, sorties.getSyncSystem());
                    preparedStatement.setString(31, sorties.getSyncSystemStr());
                    preparedStatement.setLong(32, sorties.getTestDrive());
                    preparedStatement.setString(33, sorties.getTestDriveStr());
                    preparedStatement.setString(34, sorties.getUpPilot());
                })
                .withExecutionOptions(JdbcSinkUtil.getTiDBJdbcExecutionOptions())
                .buildAtLeastOnce(JdbcSinkUtil.getTiDBJdbcConnectionOptions(TiDBDatabase.SORTIES));

        kafkaSourceDS.sinkTo(sink).name("Sorties Kafka Sinker");

        try {
            env.execute("Sorties Kafka Receiver");
        } catch (Exception e) {
            throw new ZorathosException(e, "Encounter error when executing SortiesKafkaReceiver.");
        }
    }

    public static void main(String[] args) {
        ParameterTool params = ParameterTool.fromArgs(args);
        log.info("Params: {}", params.toMap());

        String encodedJson = params.getRequired(SORTIES_BATCH_JSON.getKeyForParamsMap());
        String decodedJson = new String(Base64.getDecoder().decode(encodedJson));

        SortiesBatchReceiverConfig batchReceiverConfig = SortiesBatchReceiverConfig.builder()
                .url(params.getRequired(SORTIES_BATCH_URL.getKeyForParamsMap()))
                .json(decodedJson)
                .build();
        SortiesReceiverConfig sortiesReceiverConfig = SortiesReceiverConfig.builder()
                .baseUrl(params.getRequired(SORTIES_BASE_URL.getKeyForParamsMap()))
                .build();
        SortiesKafkaReceiver receiver = new SortiesKafkaReceiver(batchReceiverConfig, sortiesReceiverConfig);
        receiver.run();
    }
}
