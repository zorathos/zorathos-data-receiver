package org.datacenter.receiver.plan.util;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.jdbc.sink.JdbcSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.Collector;
import org.datacenter.model.base.TiDBDatabase;
import org.datacenter.model.plan.FlightCmd;
import org.datacenter.model.plan.FlightHead;
import org.datacenter.model.plan.FlightNotes;
import org.datacenter.model.plan.FlightPlan;
import org.datacenter.model.plan.FlightPlanRoot;
import org.datacenter.model.plan.FlightTask;
import org.datacenter.receiver.util.JdbcSinkUtil;

import java.sql.Date;
import java.sql.Timestamp;

/**
 * @author : [wangminan]
 * @description : 飞行计划工具
 */
public class FlightPlanSinkUtil {

    private static Sink<FlightPlanRoot> getFlightRootSink(TiDBDatabase database) {
        return JdbcSink.<FlightPlanRoot>builder()
                .withQueryStatement(
                        """
                                INSERT INTO `flight_plan_root` (
                                     `id`, `flight_date`, `flight_date_time`
                                ) VALUES (
                                     ?, ?, ?
                                );
                                """,
                        (JdbcStatementBuilder<FlightPlanRoot>) (preparedStatement, flightPlanRoot) -> {
                            preparedStatement.setString(1, flightPlanRoot.getId());
                            preparedStatement.setDate(2, flightPlanRoot.getFlightDate() == null ?
                                    null : Date.valueOf(flightPlanRoot.getFlightDate()));
                            preparedStatement.setTimestamp(3, flightPlanRoot.getFlightDateTime() == null ?
                                    null : Timestamp.valueOf(flightPlanRoot.getFlightDateTime()));
                        })
                .withExecutionOptions(JdbcSinkUtil.getTiDBJdbcExecutionOptions())
                .buildAtLeastOnce(JdbcSinkUtil.getTiDBJdbcConnectionOptions(database));
    }

    private static Sink<FlightHead> getFlightHeadSink(TiDBDatabase database) {
        return JdbcSink.<FlightHead>builder()
                .withQueryStatement(
                        """
                                INSERT INTO `flight_head` (
                                    root_id, ver, title, timeline, t_mode, plane_num, flights_time, total_time, exit_time,
                                    sun_rise_time, sun_set_time, dark_time, dawn_time, dxthh, zhshh, dwsbxh
                                ) VALUES (
                                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                                );
                                """,
                        (JdbcStatementBuilder<FlightHead>) (preparedStatement, flightHead) -> {
                            preparedStatement.setString(1, flightHead.getRootId());
                            preparedStatement.setString(2, flightHead.getVer());
                            preparedStatement.setString(3, flightHead.getTitle());
                            preparedStatement.setString(4, flightHead.getTimeline());
                            preparedStatement.setString(5, flightHead.getTMode());
                            preparedStatement.setInt(6, flightHead.getPlaneNum());
                            preparedStatement.setInt(7, flightHead.getFlightsTime());
                            preparedStatement.setInt(8, flightHead.getTotalTime());
                            preparedStatement.setInt(9, flightHead.getExitTime());
                            preparedStatement.setString(10, flightHead.getSunRiseTime());
                            preparedStatement.setString(11, flightHead.getSunSetTime());
                            preparedStatement.setString(12, flightHead.getDarkTime());
                            preparedStatement.setString(13, flightHead.getDawnTime());
                            preparedStatement.setString(14, flightHead.getDxthh());
                            preparedStatement.setString(15, flightHead.getZhshh());
                            preparedStatement.setString(16, flightHead.getDwsbxh());
                        })
                .withExecutionOptions(JdbcSinkUtil.getTiDBJdbcExecutionOptions())
                .buildAtLeastOnce(JdbcSinkUtil.getTiDBJdbcConnectionOptions(database));
    }

    private static Sink<FlightNotes> getFlightNotesSink(TiDBDatabase database) {
        return JdbcSink.<FlightNotes>builder()
                .withQueryStatement(
                        """
                                INSERT INTO `flight_notes` (
                                    root_id, note
                                ) VALUES (
                                    ?, ?
                                );
                                """,
                        (JdbcStatementBuilder<FlightNotes>) (preparedStatement, flightNotes) -> {
                            preparedStatement.setString(1, flightNotes.getRootId());
                            preparedStatement.setString(2, flightNotes.getNote());
                        })
                .withExecutionOptions(JdbcSinkUtil.getTiDBJdbcExecutionOptions())
                .buildAtLeastOnce(JdbcSinkUtil.getTiDBJdbcConnectionOptions(database));
    }

    private static Sink<FlightCmd> getFlightCmdSink(TiDBDatabase database) {
        return JdbcSink.<FlightCmd>builder()
                .withQueryStatement(
                        """
                                INSERT INTO flight_cmd (
                                    root_id, name, lb, sx
                                ) VALUES (
                                    ?, ?, ?, ?
                                );
                                """,
                        (JdbcStatementBuilder<FlightCmd>) (preparedStatement, flightCmd) -> {
                            preparedStatement.setString(1, flightCmd.getRootId());
                            preparedStatement.setString(2, flightCmd.getName());
                            preparedStatement.setString(3, flightCmd.getLb());
                            preparedStatement.setString(4, flightCmd.getSx());
                        })
                .withExecutionOptions(JdbcSinkUtil.getTiDBJdbcExecutionOptions())
                .buildAtLeastOnce(JdbcSinkUtil.getTiDBJdbcConnectionOptions(database));
    }

    private static Sink<FlightTask> getFlightTaskSink(TiDBDatabase tiDBDatabase) {
        return JdbcSink.<FlightTask>builder()
                .withQueryStatement(
                        """
                                INSERT INTO flight_task (
                                    root_id, model, code, name, rw
                                ) VALUES (
                                    ?, ?, ?, ?, ?
                                );
                                """,
                        (JdbcStatementBuilder<FlightTask>) (preparedStatement, flightTask) -> {
                            preparedStatement.setString(1, flightTask.getRootId());
                            preparedStatement.setString(2, flightTask.getModel());
                            preparedStatement.setString(3, flightTask.getCode());
                            preparedStatement.setString(4, flightTask.getName());
                            preparedStatement.setString(5, flightTask.getRw());
                        })
                .withExecutionOptions(JdbcSinkUtil.getTiDBJdbcExecutionOptions())
                .buildAtLeastOnce(JdbcSinkUtil.getTiDBJdbcConnectionOptions(tiDBDatabase));
    }

    private static Sink<FlightPlan> getFlightPlanSink(TiDBDatabase database) {
        return JdbcSink.<FlightPlan>builder()
                .withQueryStatement(
                        """
                                INSERT INTO `flight_plan` (
                                    root_id, airport_id, takeoff_time, air_battle_time, ys, plane_model, practice_number, cs, sj, jhlx, plan_time, height, ky, fz, formation_number,
                                    is_leader_plane, formation_practice, number_of_formation, front_name, front_code, front_code_name, front_nick_name,
                                    front_property, back_name, back_code, back_code_name, back_nick_name, back_property, xsms, jkys, yxyl, wqgz, grfa
                                ) VALUES (
                                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                                );
                                """,
                        (JdbcStatementBuilder<FlightPlan>) (preparedStatement, flightPlan) -> {
                            preparedStatement.setString(1, flightPlan.getRootId());
                            preparedStatement.setString(2, flightPlan.getAirportId());
                            preparedStatement.setString(3, flightPlan.getTakeoffTime());
                            preparedStatement.setString(4, flightPlan.getAirBattleTime());
                            preparedStatement.setString(5, flightPlan.getYs());
                            preparedStatement.setString(6, flightPlan.getPlaneModel());
                            preparedStatement.setString(7, flightPlan.getPracticeNumber());
                            preparedStatement.setString(8, flightPlan.getCs());
                            preparedStatement.setString(9, flightPlan.getSj());
                            preparedStatement.setString(10, flightPlan.getJhlx());
                            preparedStatement.setString(11, flightPlan.getPlanTime());
                            preparedStatement.setString(12, flightPlan.getHeight());
                            preparedStatement.setString(13, flightPlan.getKy());
                            preparedStatement.setString(14, flightPlan.getFz());
                            preparedStatement.setString(15, flightPlan.getFormationNumber());
                            preparedStatement.setString(16, flightPlan.getIsLeaderPlane());
                            preparedStatement.setString(17, flightPlan.getFormationPractice());
                            preparedStatement.setString(18, flightPlan.getNumberOfFormation());
                            preparedStatement.setString(19, flightPlan.getFrontName());
                            preparedStatement.setString(20, flightPlan.getFrontCode());
                            preparedStatement.setString(21, flightPlan.getFrontCodeName());
                            preparedStatement.setString(22, flightPlan.getFrontNickName());
                            preparedStatement.setString(23, flightPlan.getFrontProperty());
                            preparedStatement.setString(24, flightPlan.getBackName());
                            preparedStatement.setString(25, flightPlan.getBackCode());
                            preparedStatement.setString(26, flightPlan.getBackCodeName());
                            preparedStatement.setString(27, flightPlan.getBackNickName());
                            preparedStatement.setString(28, flightPlan.getBackProperty());
                            preparedStatement.setString(29, flightPlan.getXsms());
                            preparedStatement.setString(30, flightPlan.getJkys());
                            preparedStatement.setString(31, flightPlan.getYxyl());
                            preparedStatement.setString(32, flightPlan.getWqgz());
                            preparedStatement.setString(33, flightPlan.getGrfa());
                        })
                .withExecutionOptions(JdbcSinkUtil.getTiDBJdbcExecutionOptions())
                .buildAtLeastOnce(JdbcSinkUtil.getTiDBJdbcConnectionOptions(database));
    }

    public static void addMultiSinkForFlightPlanRoot(SingleOutputStreamOperator<FlightPlanRoot> sourceDs, TiDBDatabase database) {
        sourceDs.sinkTo(getFlightRootSink(database)).name("Flight root sink");
        sourceDs.map(FlightPlanRoot::getFlightHead)
                .returns(FlightHead.class)
                .sinkTo(getFlightHeadSink(database)).name("Flight head sink");
        sourceDs.map(FlightPlanRoot::getFlightNotes)
                .returns(FlightNotes.class)
                .sinkTo(getFlightNotesSink(database)).name("Flight notes sink");
        sourceDs.flatMap((FlightPlanRoot root, Collector<FlightCmd> out) -> root.getFlightCommands()
                        .forEach(out::collect))
                .returns(FlightCmd.class)
                .sinkTo(getFlightCmdSink(database)).name("Flight command sink");
        sourceDs.flatMap((FlightPlanRoot root, Collector<FlightTask> out) -> root.getFlightTasks()
                        .forEach(out::collect))
                .returns(FlightTask.class)
                .sinkTo(getFlightTaskSink(database)).name("Flight task sink");
        sourceDs.flatMap((FlightPlanRoot root, Collector<FlightPlan> out) -> root.getFlightPlans()
                        .forEach(out::collect))
                .returns(FlightPlan.class)
                .sinkTo(getFlightPlanSink(database)).name("Flight plan sink");
    }
}
