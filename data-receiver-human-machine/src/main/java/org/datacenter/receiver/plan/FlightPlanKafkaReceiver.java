package org.datacenter.receiver.plan;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;
import org.datacenter.agent.plan.FlightPlanAgent;
import org.datacenter.config.plan.FlightPlanReceiverConfig;
import org.datacenter.config.system.HumanMachineSysConfig;
import org.datacenter.exception.ZorathosException;
import org.datacenter.model.plan.FlightCmd;
import org.datacenter.model.plan.FlightHead;
import org.datacenter.model.plan.FlightNotes;
import org.datacenter.model.plan.FlightPlan;
import org.datacenter.model.plan.FlightPlanRoot;
import org.datacenter.model.plan.FlightTask;
import org.datacenter.receiver.BaseReceiver;
import org.datacenter.receiver.util.DataReceiverUtil;
import org.datacenter.receiver.util.JdbcSinkUtil;

import java.util.List;

import static org.datacenter.config.system.BaseSysConfig.humanMachineProperties;

/**
 * @author : [wangminan]
 * @description : 从Kafka中接收飞行计划数据写入TiDB
 */
@SuppressWarnings("deprecation")
@Slf4j
public class FlightPlanKafkaReceiver extends BaseReceiver {

    private final FlightPlanAgent flightPlanAgent;

    public FlightPlanKafkaReceiver(FlightPlanReceiverConfig receiverConfig) {
        // 1. 加载配置 HumanMachineSysConfig.loadConfig();
        HumanMachineSysConfig sysConfig = new HumanMachineSysConfig();
        sysConfig.loadConfig();
        this.flightPlanAgent = new FlightPlanAgent(receiverConfig);
        this.receiverConfig = receiverConfig;
    }

    @Override
    public void prepare() {
        flightPlanAgent.run();
    }

    @Override
    public void start() {
        // shutdownhook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                log.info("Flight plan kafka receiver shutting down.");
                flightPlanAgent.stop();
            } catch (Exception e) {
                throw new ZorathosException(e, "Encounter error when stopping flight plan agent. You may need to check minio to delete remote tmp file.");
            }
        }));

        // 开始从kafka获取数据
        // 引入执行环境
        StreamExecutionEnvironment env = DataReceiverUtil.prepareStreamEnv();
        DataStreamSource<FlightPlanRoot> kafkaSourceDS =
                DataReceiverUtil.getKafkaSourceDS(env, List.of(humanMachineProperties.getProperty("kafka.topic.flightPlanRoot")), FlightPlanRoot.class);

        SinkFunction<FlightPlanRoot> flightRootSink = JdbcSink.sink("""
                        INSERT INTO `flight_plan_root` (
                             `id`
                        ) VALUES (
                             ?
                        );
                        """,
                (JdbcStatementBuilder<FlightPlanRoot>) (preparedStatement, flightPlanRoot) -> preparedStatement.setString(1, flightPlanRoot.getId()),
                JdbcSinkUtil.getTiDBJdbcExecutionOptions(), JdbcSinkUtil.getTiDBJdbcConnectionOptions()
        );

        SinkFunction<FlightHead> flightHeadSink = JdbcSink.sink("""
                        INSERT INTO `flight_head` (
                            root_id, `id`, ver, title, timeline, t_mode, plane_num, flights_time, total_time, exit_time,
                            sun_rise_time, sun_set_time, dark_time, dawn_time, dxthh, zhshh, dwsbxh
                        ) VALUES (
                            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                        ) ON DUPLICATE KEY UPDATE
                            root_id = VALUES(root_id),
                            ver = VALUES(ver),
                            title = VALUES(title),
                            timeline = VALUES(timeline),
                            t_mode = VALUES(t_mode),
                            plane_num = VALUES(plane_num),
                            flights_time = VALUES(flights_time),
                            total_time = VALUES(total_time),
                            exit_time = VALUES(exit_time),
                            sun_rise_time = VALUES(sun_rise_time),
                            sun_set_time = VALUES(sun_set_time),
                            dark_time = VALUES(dark_time),
                            dawn_time = VALUES(dawn_time),
                            dxthh = VALUES(dxthh),
                            zhshh = VALUES(zhshh),
                            dwsbxh = VALUES(dwsbxh);
                        """,
                (JdbcStatementBuilder<FlightHead>) (preparedStatement, flightHead) -> {
                    preparedStatement.setString(1, flightHead.getRootId());
                    preparedStatement.setLong(2, flightHead.getId());
                    preparedStatement.setString(3, flightHead.getVer());
                    preparedStatement.setString(4, flightHead.getTitle());
                    preparedStatement.setString(5, flightHead.getTimeline());
                    preparedStatement.setString(6, flightHead.getTMode());
                    preparedStatement.setInt(7, flightHead.getPlaneNum());
                    preparedStatement.setInt(8, flightHead.getFlightsTime());
                    preparedStatement.setInt(9, flightHead.getTotalTime());
                    preparedStatement.setInt(10, flightHead.getExitTime());
                    preparedStatement.setString(11, flightHead.getSunRiseTime());
                    preparedStatement.setString(12, flightHead.getSunSetTime());
                    preparedStatement.setString(13, flightHead.getDarkTime());
                    preparedStatement.setString(14, flightHead.getDawnTime());
                    preparedStatement.setString(15, flightHead.getDxthh());
                    preparedStatement.setString(16, flightHead.getZhshh());
                    preparedStatement.setString(17, flightHead.getDwsbxh());
                },
                JdbcSinkUtil.getTiDBJdbcExecutionOptions(), JdbcSinkUtil.getTiDBJdbcConnectionOptions()
        );

        SinkFunction<FlightNotes> flightNotesSink = JdbcSink.sink("""
                        INSERT INTO `flight_notes` (
                            `root_id`, `id`, `note`
                        ) VALUES (
                            ?, ?, ?
                        ) ON DUPLICATE KEY UPDATE
                            root_id = VALUES(root_id),
                            note = VALUES(note);
                        """,
                (JdbcStatementBuilder<FlightNotes>) (preparedStatement, flightNotes) -> {
                    preparedStatement.setString(1, flightNotes.getRootId());
                    preparedStatement.setLong(2, flightNotes.getId());
                    preparedStatement.setString(3, flightNotes.getNote());
                },
                JdbcSinkUtil.getTiDBJdbcExecutionOptions(), JdbcSinkUtil.getTiDBJdbcConnectionOptions()
        );

        SinkFunction<FlightCmd> flightCmdSink = JdbcSink.sink("""
                        INSERT INTO flight_cmd (
                            root_id, id, name, lb, sx
                        ) VALUES (
                            ?, ?, ?, ?, ?
                        ) ON DUPLICATE KEY UPDATE
                            root_id = VALUES(root_id),
                            name = VALUES(name),
                            lb = VALUES(lb),
                            sx = VALUES(sx);
                        """,
                (JdbcStatementBuilder<FlightCmd>) (preparedStatement, flightCmd) -> {
                    preparedStatement.setString(1, flightCmd.getRootId());
                    preparedStatement.setLong(2, flightCmd.getId());
                    preparedStatement.setString(3, flightCmd.getName());
                    preparedStatement.setString(4, flightCmd.getLb());
                    preparedStatement.setString(5, flightCmd.getSx());
                },
                JdbcSinkUtil.getTiDBJdbcExecutionOptions(), JdbcSinkUtil.getTiDBJdbcConnectionOptions()
        );

        SinkFunction<FlightTask> flightTaskSink = JdbcSink.sink("""
                        INSERT INTO flight_task (
                            root_id, id, model, code, name, rw
                        ) VALUES (
                            ?, ?, ?, ?, ?, ?
                        ) ON DUPLICATE KEY UPDATE
                            root_id = VALUES(root_id),
                            model = VALUES(model),
                            code = VALUES(code),
                            name = VALUES(name),
                            rw = VALUES(rw);
                        """,
                (JdbcStatementBuilder<FlightTask>) (preparedStatement, flightTask) -> {
                    preparedStatement.setString(1, flightTask.getRootId());
                    preparedStatement.setLong(2, flightTask.getId());
                    preparedStatement.setString(3, flightTask.getModel());
                    preparedStatement.setString(4, flightTask.getCode());
                    preparedStatement.setString(5, flightTask.getName());
                    preparedStatement.setString(6, flightTask.getRw());
                },
                JdbcSinkUtil.getTiDBJdbcExecutionOptions(), JdbcSinkUtil.getTiDBJdbcConnectionOptions()
        );

        // 投递到数据库
        SinkFunction<FlightPlan> flightPlanSink = JdbcSink.sink("""
                        INSERT INTO `flight_plan` (
                            airport_id, takeoff_time, air_battle_time, ys, outline, lxh, cs, sj, jhlx, plan_time, height, ky, fz, bdno,
                            is_leader_plane, formation_practice, number_of_formation, front_name, front_code, front_code_name, front_nick_name,
                            front_property, back_name, back_code, back_code_name, back_nick_name, back_property, xsms, jkys, yxyl, wqgz, grfa
                        ) VALUES (
                            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                        ) ON DUPLICATE KEY UPDATE
                            airport_id = VALUES(airport_id), takeoff_time = VALUES(takeoff_time), air_battle_time = VALUES(air_battle_time), ys = VALUES(ys),
                            outline = VALUES(outline), lx = VALUES(lx), cs = VALUES(cs), sj = VALUES(sj), jhlx = VALUES(jhlx), plan_time = VALUES(plan_time),
                            height = VALUES(height), ky = VALUES(ky), fz = VALUES(fz), bdno = VALUES(bdno), is_leader_plane = VALUES(is_leader_plane),
                            formation_practice = VALUES(formation_practice), number_of_formation = VALUES(number_of_formation), front_name = VALUES(front_name),
                            front_code = VALUES(front_code), front_code_name = VALUES(front_code_name), front_nick_name = VALUES(front_nick_name),
                            front_property = VALUES(front_property), back_name = VALUES(back_name), back_code = VALUES(back_code), back_code_name = VALUES(back_code_name),
                            back_nick_name = VALUES(back_nick_name), back_property = VALUES(back_property), xsms = VALUES(xsms), jkys = VALUES(jkys),
                            yxyl = VALUES(yxyl), wqgz = VALUES(wqgz), grfa = VALUES(grfa);
                        """,
                (JdbcStatementBuilder<FlightPlan>) (preparedStatement, flightPlan) -> {
                    // 全是string 枚举即可
                    preparedStatement.setString(1, flightPlan.getAirportId());
                    preparedStatement.setString(2, flightPlan.getTakeoffTime());
                    preparedStatement.setString(3, flightPlan.getAirBattleTime());
                    preparedStatement.setString(4, flightPlan.getYs());
                    preparedStatement.setString(5, flightPlan.getOutline());
                    preparedStatement.setString(6, flightPlan.getLxh());
                    preparedStatement.setString(7, flightPlan.getCs());
                    preparedStatement.setString(8, flightPlan.getSj());
                    preparedStatement.setString(9, flightPlan.getJhlx());
                    preparedStatement.setString(10, flightPlan.getPlanTime());
                    preparedStatement.setString(11, flightPlan.getHeight());
                    preparedStatement.setString(12, flightPlan.getKy());
                    preparedStatement.setString(13, flightPlan.getFz());
                    preparedStatement.setString(14, flightPlan.getBdno());
                    preparedStatement.setString(15, flightPlan.getIsLeaderPlane());
                    preparedStatement.setString(16, flightPlan.getFormationPractice());
                    preparedStatement.setString(17, flightPlan.getNumberOfFormation());
                    preparedStatement.setString(18, flightPlan.getFrontName());
                    preparedStatement.setString(19, flightPlan.getFrontCode());
                    preparedStatement.setString(20, flightPlan.getFrontCodeName());
                    preparedStatement.setString(21, flightPlan.getFrontNickName());
                    preparedStatement.setString(22, flightPlan.getFrontProperty());
                    preparedStatement.setString(23, flightPlan.getBackName());
                    preparedStatement.setString(24, flightPlan.getBackCode());
                    preparedStatement.setString(25, flightPlan.getBackCodeName());
                    preparedStatement.setString(26, flightPlan.getBackNickName());
                    preparedStatement.setString(27, flightPlan.getBackProperty());
                    preparedStatement.setString(28, flightPlan.getXsms());
                    preparedStatement.setString(29, flightPlan.getJkys());
                    preparedStatement.setString(30, flightPlan.getYxyl());
                    preparedStatement.setString(31, flightPlan.getWqgz());
                    preparedStatement.setString(32, flightPlan.getGrfa());
                },
                JdbcSinkUtil.getTiDBJdbcExecutionOptions(), JdbcSinkUtil.getTiDBJdbcConnectionOptions()
        );

        // 重复使用datastream flink在每一次对datastream操作之后都会new一个新的对象 所以不用担心反复消费的问题
        kafkaSourceDS.addSink(flightRootSink);
        kafkaSourceDS.map(FlightPlanRoot::getFlightHead).addSink(flightHeadSink);
        kafkaSourceDS.map(FlightPlanRoot::getFlightNotes).addSink(flightNotesSink);
        kafkaSourceDS.flatMap((FlightPlanRoot root, Collector<FlightCmd> out) -> root.getFlightCommands()
                        .forEach(out::collect))
                .addSink(flightCmdSink);
        kafkaSourceDS.flatMap((FlightPlanRoot root, Collector<FlightTask> out) -> root.getFlightTasks()
                        .forEach(out::collect))
                .addSink(flightTaskSink);
        kafkaSourceDS.flatMap((FlightPlanRoot root, Collector<FlightPlan> out) -> root.getFlightPlans()
                        .forEach(out::collect))
                .addSink(flightPlanSink);

        try {
            env.execute();
        } catch (Exception e) {
            throw new ZorathosException(e, "Encounter error when sinking flight plan data to tidb.");
        }
    }

    public static void main(String[] args) {
        ObjectMapper mapper = new ObjectMapper();
        FlightPlanReceiverConfig receiverConfig;
        try {
            receiverConfig = mapper.readValue(args[0], FlightPlanReceiverConfig.class);
        } catch (JsonProcessingException e) {
            throw new ZorathosException(e, "Error occurs while converting flight plan receiver config to json string.");
        }
        if (receiverConfig != null) {
            FlightPlanKafkaReceiver receiver = new FlightPlanKafkaReceiver(receiverConfig);
            receiver.run();
        }
    }
}
