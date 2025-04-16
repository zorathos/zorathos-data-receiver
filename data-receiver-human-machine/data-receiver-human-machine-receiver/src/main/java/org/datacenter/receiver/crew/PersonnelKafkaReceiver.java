package org.datacenter.receiver.crew;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.jdbc.sink.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.datacenter.agent.personnel.PersonnelAgent;
import org.datacenter.config.HumanMachineConfig;
import org.datacenter.config.receiver.PersonnelAndPlanLoginConfig;
import org.datacenter.config.receiver.crew.PersonnelReceiverConfig;
import org.datacenter.exception.ZorathosException;
import org.datacenter.model.base.TiDBDatabase;
import org.datacenter.model.crew.PersonnelInfo;
import org.datacenter.receiver.BaseReceiver;
import org.datacenter.receiver.util.DataReceiverUtil;
import org.datacenter.receiver.util.JdbcSinkUtil;

import java.sql.Date;
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

    public PersonnelKafkaReceiver(PersonnelAndPlanLoginConfig loginConfig, PersonnelReceiverConfig receiverConfig) {
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
        // 投递到数据库 写sql时使用upsert语法
        Sink<PersonnelInfo> sink = JdbcSink.<PersonnelInfo>builder()
                .withQueryStatement("""
                        INSERT INTO `personnel_info` (
                            unit_code, unit, personal_identifier, name, position, sex, appointment_date, native_place, family_background,
                            education_level, birthday, enlistment_date, rating_date, graduate_college, graduation_date, military_rank,
                            pilot_role, flight_level, current_aircraft_type, pxh, code_name, bm, code_character, is_air_combat_commander,
                            flight_outline, lead_pilot, command_level_daytime, command_level_nighttime, instructor, theoretical_instructor,
                            zbzt, is_trainee, is_instructor, qb, last_parachute_time_land, last_parachute_time_water, modification_time,
                            total_time_history, total_time_current_year, total_teaching_time_history
                        ) VALUES (
                            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                        ) ON DUPLICATE KEY UPDATE
                            unit_code = VALUES(unit_code), unit = VALUES(unit), name = VALUES(name),
                            position = VALUES(position), sex = VALUES(sex), appointment_date = VALUES(appointment_date),
                            native_place = VALUES(native_place), family_background = VALUES(family_background),
                            education_level = VALUES(education_level), birthday = VALUES(birthday), enlistment_date = VALUES(enlistment_date), rating_date = VALUES(rating_date),
                            graduate_college = VALUES(graduate_college), graduation_date = VALUES(graduation_date), military_rank = VALUES(military_rank),
                            pilot_role = VALUES(pilot_role), flight_level = VALUES(flight_level), current_aircraft_type = VALUES(current_aircraft_type), pxh = VALUES(pxh),
                            code_name = VALUES(code_name), bm = VALUES(bm), code_character = VALUES(code_character), is_air_combat_commander = VALUES(is_air_combat_commander),
                            flight_outline = VALUES(flight_outline), lead_pilot = VALUES(lead_pilot), command_level_daytime = VALUES(command_level_daytime),
                            command_level_nighttime = VALUES(command_level_nighttime), instructor = VALUES(instructor), theoretical_instructor = VALUES(theoretical_instructor),
                            zbzt = VALUES(zbzt), is_trainee = VALUES(is_trainee), is_instructor = VALUES(is_instructor), qb = VALUES(qb),
                            last_parachute_time_land = VALUES(last_parachute_time_land), last_parachute_time_water = VALUES(last_parachute_time_water),
                            modification_time = VALUES(modification_time), total_time_history = VALUES(total_time_history), total_time_current_year = VALUES(total_time_current_year),
                            total_teaching_time_history = VALUES(total_teaching_time_history);
                        """, (JdbcStatementBuilder<PersonnelInfo>) (preparedStatement, personnelInfo) -> {
                    preparedStatement.setString(1, personnelInfo.getUnitCode());
                    preparedStatement.setString(2, personnelInfo.getUnit());
                    preparedStatement.setString(3, personnelInfo.getPersonalIdentifier());
                    preparedStatement.setString(4, personnelInfo.getName());
                    preparedStatement.setString(5, personnelInfo.getPosition());
                    preparedStatement.setString(6, personnelInfo.getSex()); // 添加缺失的sex字段
                    preparedStatement.setDate(7, personnelInfo.getAppointmentDate() == null ? null :
                            Date.valueOf(personnelInfo.getAppointmentDate()));
                    preparedStatement.setString(8, personnelInfo.getNativePlace());
                    preparedStatement.setString(9, personnelInfo.getFamilyBackground());
                    preparedStatement.setString(10, personnelInfo.getEducationLevel());
                    preparedStatement.setDate(11, personnelInfo.getBirthday() == null ? null :
                            Date.valueOf(personnelInfo.getBirthday()));
                    preparedStatement.setDate(12, personnelInfo.getEnlistmentDate() == null ? null :
                            Date.valueOf(personnelInfo.getEnlistmentDate()));
                    preparedStatement.setDate(13, personnelInfo.getRatingDate() == null ? null :
                            Date.valueOf(personnelInfo.getRatingDate()));
                    preparedStatement.setString(14, personnelInfo.getGraduateCollege());
                    preparedStatement.setDate(15, personnelInfo.getGraduationDate() == null ? null :
                            Date.valueOf(personnelInfo.getGraduationDate()));
                    preparedStatement.setString(16, personnelInfo.getMilitaryRank());
                    preparedStatement.setString(17, personnelInfo.getPilotRole());
                    preparedStatement.setString(18, personnelInfo.getFlightLevel());
                    preparedStatement.setString(19, personnelInfo.getCurrentAircraftType());
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
                    preparedStatement.setDate(35, personnelInfo.getLastParachuteTimeLand() == null ? null :
                            Date.valueOf(personnelInfo.getLastParachuteTimeLand()));
                    preparedStatement.setDate(36, personnelInfo.getLastParachuteTimeWater() == null ? null :
                            Date.valueOf(personnelInfo.getLastParachuteTimeWater()));
                    preparedStatement.setDate(37, personnelInfo.getModificationTime() == null ? null :
                            Date.valueOf(personnelInfo.getModificationTime()));
                    preparedStatement.setString(38, personnelInfo.getTotalTimeHistory());
                    preparedStatement.setString(39, personnelInfo.getTotalTimeCurrentYear());
                    preparedStatement.setString(40, personnelInfo.getTotalTeachingTimeHistory());
                })
                .withExecutionOptions(JdbcSinkUtil.getTiDBJdbcExecutionOptions())
                .buildAtLeastOnce(JdbcSinkUtil.getTiDBJdbcConnectionOptions(TiDBDatabase.HUMAN_MACHINE));

        kafkaSourceDS.sinkTo(sink).name("Personnel kafka sink.");
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
        PersonnelReceiverConfig receiverConfig = PersonnelReceiverConfig.builder()
                .url(params.getRequired(PERSONNEL_AND_PLAN_LOGIN_URL.getKeyForParamsMap()))
                .build();
        PersonnelKafkaReceiver personnelKafkaReceiver = new PersonnelKafkaReceiver(loginConfig, receiverConfig);
        personnelKafkaReceiver.run();
    }
}
