package org.datacenter.receiver.crew;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.datacenter.agent.personnel.PersonnelAgent;
import org.datacenter.config.personnel.PersonnelReceiverConfig;
import org.datacenter.config.system.HumanMachineSysConfig;
import org.datacenter.exception.ZorathosException;
import org.datacenter.model.crew.PersonnelInfo;
import org.datacenter.receiver.BaseReceiver;
import org.datacenter.receiver.util.DataReceiverUtil;
import org.datacenter.receiver.util.JdbcSinkUtil;

import java.io.IOException;
import java.sql.Date;
import java.util.List;
import java.util.Timer;
import java.util.concurrent.CountDownLatch;

import static org.datacenter.config.system.BaseSysConfig.humanMachineProperties;

/**
 * @author : [wangminan]
 * @description : 人员数据Kafka接收器
 */
@Slf4j
@SuppressWarnings("deprecation")
public class PersonnelKafkaReceiver extends BaseReceiver {

    private final PersonnelAgent personnelAgent;

    public PersonnelKafkaReceiver(PersonnelReceiverConfig receiverConfig) {
        // 1. 加载配置 HumanMachineSysConfig.loadConfig();
        HumanMachineSysConfig sysConfig = new HumanMachineSysConfig();
        sysConfig.loadConfig();
        this.receiverConfig = receiverConfig;
        // 2. 初始化人员Agent
        this.personnelAgent = new PersonnelAgent(receiverConfig);
    }

    @Override
    public void prepare() {
        personnelAgent.run();
        awaitAgentRunning(personnelAgent);
    }

    @Override
    public void start() {
        // shutdownhook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                log.info("Flight plan kafka receiver shutting down.");
                personnelAgent.stop();
            } catch (Exception e) {
                throw new ZorathosException(e, "Encounter error when stopping personnel agent. You may need to check minio to delete remote tmp file.");
            }
        }));

        // 开始从kafka获取数据
        // 引入执行环境
        StreamExecutionEnvironment env = DataReceiverUtil.prepareStreamEnv();
        DataStreamSource<PersonnelInfo> kafkaSourceDS =
                DataReceiverUtil.getKafkaSourceDS(env, List.of(humanMachineProperties.getProperty("kafka.topic.personnel")), PersonnelInfo.class);
        // 投递到数据库 写sql时使用upsert语法
        SinkFunction<PersonnelInfo> sinkFunction = JdbcSink.sink(
                """
                        INSERT INTO `personnel_info` (
                            unit_code, unit, personal_identifier, name, position, appointment_date, native_place, family_background,
                            education_level, birthday, enlistment_date, rating_date, graduate_college, graduation_date, military_rank,
                            pilot_role, flight_level, current_aircraft_model, pxh, code_name, bm, code_character, is_air_combat_commander,
                            flight_outline, lead_pilot, command_level_daytime, command_level_nighttime, instructor, theoretical_instructor,
                            zbzt, is_trainee, is_instructor, qb, last_parachute_time_land, last_parachute_time_water, modification_time,
                            total_time_history, total_time_current_year, total_teaching_time_history
                        ) VALUES (
                            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                        ) ON DUPLICATE KEY UPDATE
                            unit_code = VALUES(unit_code), unit = VALUES(unit), personal_identifier = VALUES(personal_identifier), name = VALUES(name),
                            position = VALUES(position), appointment_date = VALUES(appointment_date), native_place = VALUES(native_place), family_background = VALUES(family_background),
                            education_level = VALUES(education_level), birthday = VALUES(birthday), enlistment_date = VALUES(enlistment_date), rating_date = VALUES(rating_date),
                            graduate_college = VALUES(graduate_college), graduation_date = VALUES(graduation_date), military_rank = VALUES(military_rank),
                            pilot_role = VALUES(pilot_role), flight_level = VALUES(flight_level), current_aircraft_model = VALUES(current_aircraft_model), pxh = VALUES(pxh),
                            code_name = VALUES(code_name), bm = VALUES(bm), code_character = VALUES(code_character), is_air_combat_commander = VALUES(is_air_combat_commander),
                            flight_outline = VALUES(flight_outline), lead_pilot = VALUES(lead_pilot), command_level_daytime = VALUES(command_level_daytime),
                            command_level_nighttime = VALUES(command_level_nighttime), instructor = VALUES(instructor), theoretical_instructor = VALUES(theoretical_instructor),
                            zbzt = VALUES(zbzt), is_trainee = VALUES(is_trainee), is_instructor = VALUES(is_instructor), qb = VALUES(qb),
                            last_parachute_time_land = VALUES(last_parachute_time_land), last_parachute_time_water = VALUES(last_parachute_time_water),
                            modification_time = VALUES(modification_time), total_time_history = VALUES(total_time_history), total_time_current_year = VALUES(total_time_current_year),
                            total_teaching_time_history = VALUES(total_teaching_time_history);
                        """,
                (JdbcStatementBuilder<PersonnelInfo>) (preparedStatement, personnelInfo) -> {
                    preparedStatement.setString(1, personnelInfo.getUnitCode());
                    preparedStatement.setString(2, personnelInfo.getUnit());
                    preparedStatement.setString(3, personnelInfo.getPersonalIdentifier());
                    preparedStatement.setString(4, personnelInfo.getName());
                    preparedStatement.setString(5, personnelInfo.getPosition());
                    preparedStatement.setDate(6, Date.valueOf(personnelInfo.getAppointmentDate()));
                    preparedStatement.setString(7, personnelInfo.getNativePlace());
                    preparedStatement.setString(8, personnelInfo.getFamilyBackground());
                    preparedStatement.setString(9, personnelInfo.getEducationLevel());
                    preparedStatement.setDate(10, Date.valueOf(personnelInfo.getBirthday()));
                    preparedStatement.setDate(11, Date.valueOf(personnelInfo.getEnlistmentDate()));
                    preparedStatement.setDate(12, Date.valueOf(personnelInfo.getRatingDate()));
                    preparedStatement.setString(13, personnelInfo.getGraduateCollege());
                    preparedStatement.setDate(14, Date.valueOf(personnelInfo.getGraduationDate()));
                    preparedStatement.setString(15, personnelInfo.getMilitaryRank());
                    preparedStatement.setString(16, personnelInfo.getPilotRole());
                    preparedStatement.setString(17, personnelInfo.getFlightLevel());
                    preparedStatement.setString(18, personnelInfo.getCurrentAircraftModel());
                    preparedStatement.setString(19, personnelInfo.getPxh());
                    preparedStatement.setString(20, personnelInfo.getCodeName());
                    preparedStatement.setString(21, personnelInfo.getBm());
                    preparedStatement.setString(22, personnelInfo.getCodeCharacter());
                    preparedStatement.setString(23, personnelInfo.getIsAirCombatCommander());
                    preparedStatement.setString(24, personnelInfo.getFlightOutline());
                    preparedStatement.setString(25, personnelInfo.getLeadPilot());
                    preparedStatement.setString(26, personnelInfo.getCommandLevelDaytime());
                    preparedStatement.setString(27, personnelInfo.getCommandLevelNighttime());
                    preparedStatement.setString(28, personnelInfo.getInstructor());
                    preparedStatement.setString(29, personnelInfo.getTheoreticalInstructor());
                    preparedStatement.setString(30, personnelInfo.getZbzt());
                    preparedStatement.setString(31, personnelInfo.getIsTrainee());
                    preparedStatement.setString(32, personnelInfo.getIsInstructor());
                    preparedStatement.setString(33, personnelInfo.getQb());
                    preparedStatement.setDate(34, Date.valueOf(personnelInfo.getLastParachuteTimeLand()));
                    preparedStatement.setDate(35, Date.valueOf(personnelInfo.getLastParachuteTimeWater()));
                    preparedStatement.setDate(36, Date.valueOf(personnelInfo.getModificationTime()));
                    preparedStatement.setString(37, personnelInfo.getTotalTimeHistory());
                    preparedStatement.setString(38, personnelInfo.getTotalTimeCurrentYear());
                    preparedStatement.setString(39, personnelInfo.getTotalTeachingTimeHistory());
                },
                JdbcSinkUtil.getTiDBJdbcExecutionOptions(), JdbcSinkUtil.getTiDBJdbcConnectionOptions()
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
     *
     * @param args 参数 第一个为接收器参数 第二个为持久化器参数
     */
    public static void main(String[] args) {
        ObjectMapper mapper = new ObjectMapper();
        PersonnelReceiverConfig receiverConfig;

        try {
            receiverConfig = mapper.readValue(args[0], PersonnelReceiverConfig.class);
        } catch (IOException e) {
            throw new ZorathosException(e, "Error occurs while converting personnel receiver config to json string.");
        }
        if (receiverConfig != null) {
            PersonnelKafkaReceiver personnelKafkaReceiver = new PersonnelKafkaReceiver(receiverConfig);
            personnelKafkaReceiver.run();
        }
    }
}
