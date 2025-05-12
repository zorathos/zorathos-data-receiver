package org.datacenter.receiver.crew.util;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.jdbc.sink.JdbcSink;
import org.datacenter.model.base.TiDBDatabase;
import org.datacenter.model.crew.PersonnelInfo;
import org.datacenter.receiver.util.JdbcSinkUtil;

import java.sql.Date;

/**
 * @author : [wangminan]
 * @description : 人员接收工具
 */
public class PersonnelSinkUtil {

    /**
     * 投递到数据库 写sql时使用upsert语法
     * 如果把 PersonnelJdbcSink 写作一个static变量，在minicluster下不会报错
     * 但在集群下会报java.lang.NoClassDefFoundError: Could not initialize class org.datacenter.receiver.crew.util.PersonnelSinkUtil
     * 通过反射stack我估摸着是flink在加载jar包的时候用反射加载了 PersonnelSinkUtil static变量会第一时间初始化
     * 但是 PersonnelJdbcSink 依赖于JdbcSinkUtil 也就依赖于懒加载的 {@link org.datacenter.config.HumanMachineConfig} 所以会报properties空
     */
    public static Sink<PersonnelInfo> getPilotJdbcSink(String importId) {
        return JdbcSink.<PersonnelInfo>builder()
                .withQueryStatement("""
                        INSERT INTO `personnel_info` (
                            unit_code, unit, personal_identifier, name, position, sex, appointment_date, native_place, family_background,
                            education_level, birthday, enlistment_date, rating_date, graduate_college, graduation_date, military_rank,
                            pilot_role, flight_level, current_aircraft_type, pxh, code_name, bm, code_character, is_air_combat_commander,
                            flight_outline, lead_pilot, command_level_daytime, command_level_nighttime, instructor, theoretical_instructor,
                            zbzt, is_trainee, is_instructor, qb, last_parachute_time_land, last_parachute_time_water, modification_time,
                            total_time_history, total_time_current_year, total_teaching_time_history, import_id
                        ) VALUES (
                            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                        ) ON DUPLICATE KEY UPDATE
                            import_id = VALUES(import_id), unit_code = VALUES(unit_code), unit = VALUES(unit), name = VALUES(name),
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
                    preparedStatement.setString(41, importId);
                })
                .withExecutionOptions(JdbcSinkUtil.getTiDBJdbcExecutionOptions())
                .buildAtLeastOnce(JdbcSinkUtil.getTiDBJdbcConnectionOptions(TiDBDatabase.PERSONNEL));
    }
}
