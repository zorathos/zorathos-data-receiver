package org.datacenter.receiver.crew;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.datacenter.agent.personnel.PersonnelAgent;
import org.datacenter.config.system.HumanMachineSysConfig;
import org.datacenter.exception.ZorathosException;
import org.datacenter.model.crew.PersonnelInfo;
import org.datacenter.receiver.BaseReceiver;

import java.io.IOException;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;

import static org.datacenter.config.system.BaseSysConfig.humanMachineProperties;

/**
 * @author : [wangminan]
 * @description : 人员数据Kafka接收器
 */
@SuppressWarnings("deprecation")
public class PersonnelKafkaReceiver extends BaseReceiver {

    private final PersonnelAgent personnelAgent;

    public PersonnelKafkaReceiver() {
        // 1. 加载配置 HumanMachineSysConfig.loadConfig();
        HumanMachineSysConfig sysConfig = new HumanMachineSysConfig();
        sysConfig.loadConfig();
        // 2. 初始化人员Agent
        this.personnelAgent = new PersonnelAgent();
    }

    @Override
    public void prepare() {
        personnelAgent.run();
    }

    @Override
    public void start() {
        // 开始从kafka获取数据
        // 引入执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 精确一次要求开启checkpoint
        env.enableCheckpointing(
                Long.parseLong(humanMachineProperties.getProperty("flink.kafka.checkpoint.interval")),
                CheckpointingMode.EXACTLY_ONCE);
        // 数据源
        KafkaSource<PersonnelInfo> kafkaSource = KafkaSource.<PersonnelInfo>builder()
                .setBootstrapServers(humanMachineProperties.getProperty("kafka.bootstrap.servers"))
                .setGroupId(humanMachineProperties.getProperty("kafka.consumer.group-id"))
                .setTopics(humanMachineProperties.getProperty("kafka.topic.personnel"))
                .setValueOnlyDeserializer(new AbstractDeserializationSchema<>() {
                    @Override
                    public PersonnelInfo deserialize(byte[] message) throws IOException {
                        ObjectMapper mapper = new ObjectMapper();
                        return mapper.readValue(message, PersonnelInfo.class);
                    }
                })
                .setStartingOffsets(OffsetsInitializer.latest())
                .build();
        DataStreamSource<PersonnelInfo> kafkaSourceDS = env
                .fromSource(kafkaSource,
                        WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(3)),
                        "kafkaSource");
        // 投递到数据库
        SinkFunction<PersonnelInfo> sinkFunction = JdbcSink.sink(
                """
                INSERT INTO `personnel_info` (
                    id, unit_code, unit, personal_identifier, name, position, appointment_date, native_place, family_background,
                    education_level, birthday, enlistment_date, rating_date, graduate_college, graduation_date, military_rank,
                    pilot_role, flight_level, current_aircraft_model, pxh, code_name, bm, code_character, is_air_combat_commander,
                    flight_outline, lead_pilot, command_level_daytime, command_level_nighttime, instructor, theoretical_instructor,
                    zbzt, is_trainee, is_instructor, qb, last_parachute_time_land, last_parachute_time_water, modification_time,
                    total_time_history, total_time_current_year, total_teaching_time_history
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                );
                """,
                (JdbcStatementBuilder<PersonnelInfo>) (preparedStatement, personnelInfo) -> {
                    preparedStatement.setString(1, personnelInfo.getId());
                    preparedStatement.setString(2, personnelInfo.getUnitCode());
                    preparedStatement.setString(3, personnelInfo.getUnit());
                    preparedStatement.setString(4, personnelInfo.getPersonalIdentifier());
                    preparedStatement.setString(5, personnelInfo.getName());
                    preparedStatement.setString(6, personnelInfo.getPosition());
                    preparedStatement.setDate(7, Date.valueOf(personnelInfo.getAppointmentDate()));
                    preparedStatement.setString(8, personnelInfo.getNativePlace());
                    preparedStatement.setString(9, personnelInfo.getFamilyBackground());
                    preparedStatement.setString(10, personnelInfo.getEducationLevel());
                    preparedStatement.setDate(11, Date.valueOf(personnelInfo.getBirthday()));
                    preparedStatement.setDate(12, Date.valueOf(personnelInfo.getEnlistmentDate()));
                    preparedStatement.setDate(13, Date.valueOf(personnelInfo.getRatingDate()));
                    preparedStatement.setString(14, personnelInfo.getGraduateCollege());
                    preparedStatement.setDate(15, Date.valueOf(personnelInfo.getGraduationDate()));
                    preparedStatement.setString(16, personnelInfo.getMilitaryRank());
                    preparedStatement.setString(17, personnelInfo.getPilotRole());
                    preparedStatement.setString(18, personnelInfo.getFlightLevel());
                    preparedStatement.setString(19, personnelInfo.getCurrentAircraftModel());
                    preparedStatement.setString(20, personnelInfo.getPxh());
                    preparedStatement.setString(21, personnelInfo.getCodeName());
                    preparedStatement.setString(22, personnelInfo.getBm());
                    preparedStatement.setString(23, personnelInfo.getCodeCharacter());
                    preparedStatement.setString(24, personnelInfo.getIsAirCombatCommander());
                    preparedStatement.setString(25, personnelInfo.getFlightOutline());
                    preparedStatement.setString(26, personnelInfo.getLeadPilot());
                    preparedStatement.setString(27, personnelInfo.getCommandLevelDaytime());
                    preparedStatement.setString(28, personnelInfo.getCommandLevelNighttime());
                    preparedStatement.setString(29, personnelInfo.getInstructor());
                    preparedStatement.setString(30, personnelInfo.getTheoreticalInstructor());
                    preparedStatement.setString(31, personnelInfo.getZbzt());
                    preparedStatement.setString(32, personnelInfo.getIsTrainee());
                    preparedStatement.setString(33, personnelInfo.getIsInstructor());
                    preparedStatement.setString(34, personnelInfo.getQb());
                    preparedStatement.setDate(35, Date.valueOf(personnelInfo.getLastParachuteTimeLand()));
                    preparedStatement.setDate(36, Date.valueOf(personnelInfo.getLastParachuteTimeWater()));
                    preparedStatement.setDate(37, Date.valueOf(personnelInfo.getModificationTime()));
                    preparedStatement.setString(38, personnelInfo.getTotalTimeHistory());
                    preparedStatement.setString(39, personnelInfo.getTotalTimeCurrentYear());
                    preparedStatement.setString(40, personnelInfo.getTotalTeachingTimeHistory());
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(Integer.parseInt(humanMachineProperties.getProperty("flink.jdbc.sinker.batchSize")))
                        .withBatchIntervalMs(Integer.parseInt(humanMachineProperties.getProperty("flink.jdbc.sinker.batchInterval")))
                        .withMaxRetries(Integer.parseInt(humanMachineProperties.getProperty("flink.jdbc.sinker.maxRetries")))
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(humanMachineProperties.getProperty("tidb.url.flightPlan"))
                        .withDriverName(humanMachineProperties.getProperty("tidb.driverName"))
                        .withUsername(humanMachineProperties.getProperty("tidb.username"))
                        .withPassword(humanMachineProperties.getProperty("tidb.password"))
                        .withConnectionCheckTimeoutSeconds(Integer.parseInt(humanMachineProperties.getProperty("tidb.connectionCheckTimeoutSeconds")))
                        .build()
        );
        kafkaSourceDS.addSink(sinkFunction);
        try {
            env.execute();
        } catch (Exception e) {
            throw new ZorathosException(e, "Encounter error when sinking personnel data to tidb.");
        }
    }

    /**
     * 被flink调用的主函数
     * @param args 参数 第一个为接收器参数 第二个为持久化器参数
     */
    public static void main(String[] args) {
        PersonnelKafkaReceiver personnelKafkaReceiver = new PersonnelKafkaReceiver();
        personnelKafkaReceiver.run();
    }
}
