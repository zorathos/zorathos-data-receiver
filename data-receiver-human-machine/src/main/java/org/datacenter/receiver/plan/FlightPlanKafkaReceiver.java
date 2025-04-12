package org.datacenter.receiver.plan;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.jdbc.sink.JdbcSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.datacenter.agent.plan.FlightPlanAgent;
import org.datacenter.config.HumanMachineConfig;
import org.datacenter.config.receiver.PersonnelAndPlanLoginConfig;
import org.datacenter.config.receiver.plan.FlightPlanReceiverConfig;
import org.datacenter.exception.ZorathosException;
import org.datacenter.model.base.TiDBDatabase;
import org.datacenter.model.plan.FlightCmd;
import org.datacenter.model.plan.FlightHead;
import org.datacenter.model.plan.FlightNotes;
import org.datacenter.model.plan.FlightPlan;
import org.datacenter.model.plan.FlightPlanRoot;
import org.datacenter.model.plan.FlightTask;
import org.datacenter.receiver.BaseReceiver;
import org.datacenter.receiver.util.DataReceiverUtil;
import org.datacenter.receiver.util.JdbcSinkUtil;

import java.sql.Date;
import java.util.Base64;
import java.util.List;

import static org.datacenter.config.keys.HumanMachineSysConfigKey.KAFKA_TOPIC_FLIGHT_PLAN_ROOT;

/**
 * @author : [wangminan]
 * @description : 从Kafka中接收飞行计划数据写入TiDB
 */
@Slf4j
public class FlightPlanKafkaReceiver extends BaseReceiver {

    private final FlightPlanAgent flightPlanAgent;

    public FlightPlanKafkaReceiver(PersonnelAndPlanLoginConfig loginConfig,
                                   FlightPlanReceiverConfig flightPlanReceiverConfig) {
        // 1. 加载配置 HumanMachineConfig.loadConfig();
        HumanMachineConfig sysConfig = new HumanMachineConfig();
        sysConfig.loadConfig();
        this.flightPlanAgent = new FlightPlanAgent(loginConfig, flightPlanReceiverConfig);
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

        Sink<FlightPlanRoot> flightRootSink = JdbcSink.<FlightPlanRoot>builder()
                .withQueryStatement(
                        """
                                INSERT INTO `flight_plan_root` (
                                     `id`, `flight_date`
                                ) VALUES (
                                     ?, ?
                                );
                                """,
                        (JdbcStatementBuilder<FlightPlanRoot>) (preparedStatement, flightPlanRoot) -> {
                            preparedStatement.setString(1, flightPlanRoot.getId());
                            preparedStatement.setDate(2, Date.valueOf(flightPlanRoot.getFlightDate()));
                        })
                .withExecutionOptions(JdbcSinkUtil.getTiDBJdbcExecutionOptions())
                .buildAtLeastOnce(JdbcSinkUtil.getTiDBJdbcConnectionOptions(TiDBDatabase.FLIGHT_PLAN));

        Sink<FlightHead> flightHeadSink = JdbcSink.<FlightHead>builder()
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
                .buildAtLeastOnce(JdbcSinkUtil.getTiDBJdbcConnectionOptions(TiDBDatabase.FLIGHT_PLAN));

        Sink<FlightNotes> flightNotesSink = JdbcSink.<FlightNotes>builder()
                .withQueryStatement(
                        """
                                INSERT INTO `flight_notes` (
                                    `root_id`, `note`
                                ) VALUES (
                                    ?, ?
                                );
                                """,
                        (JdbcStatementBuilder<FlightNotes>) (preparedStatement, flightNotes) -> {
                            preparedStatement.setString(1, flightNotes.getRootId());
                            preparedStatement.setString(2, flightNotes.getNote());
                        })
                .withExecutionOptions(JdbcSinkUtil.getTiDBJdbcExecutionOptions())
                .buildAtLeastOnce(JdbcSinkUtil.getTiDBJdbcConnectionOptions(TiDBDatabase.FLIGHT_PLAN));

        Sink<FlightCmd> flightCmdSink = JdbcSink.<FlightCmd>builder()
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
                .buildAtLeastOnce(JdbcSinkUtil.getTiDBJdbcConnectionOptions(TiDBDatabase.FLIGHT_PLAN));

        Sink<FlightTask> flightTaskSink = JdbcSink.<FlightTask>builder()
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
                .buildAtLeastOnce(JdbcSinkUtil.getTiDBJdbcConnectionOptions(TiDBDatabase.FLIGHT_PLAN));

        Sink<FlightPlan> flightPlanSink = JdbcSink.<FlightPlan>builder()
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
                .buildAtLeastOnce(JdbcSinkUtil.getTiDBJdbcConnectionOptions(TiDBDatabase.FLIGHT_PLAN));

        // 重复使用datastream flink在每一次对datastream操作之后都会new一个新的对象 所以不用担心反复消费的问题
        kafkaSourceDS.sinkTo(flightRootSink).name("Flight root sink");
        kafkaSourceDS.map(FlightPlanRoot::getFlightHead)
                .returns(FlightHead.class)
                .sinkTo(flightHeadSink).name("Flight head sink");
        kafkaSourceDS.map(FlightPlanRoot::getFlightNotes)
                .returns(FlightNotes.class)
                .sinkTo(flightNotesSink).name("Flight notes sink");
        kafkaSourceDS.flatMap((FlightPlanRoot root, Collector<FlightCmd> out) -> root.getFlightCommands()
                        .forEach(out::collect))
                .returns(FlightCmd.class)
                .sinkTo(flightCmdSink).name("Flight command sink");
        kafkaSourceDS.flatMap((FlightPlanRoot root, Collector<FlightTask> out) -> root.getFlightTasks()
                        .forEach(out::collect))
                .returns(FlightTask.class)
                .sinkTo(flightTaskSink).name("Flight task sink");
        kafkaSourceDS.flatMap((FlightPlanRoot root, Collector<FlightPlan> out) -> root.getFlightPlans()
                        .forEach(out::collect))
                .returns(FlightPlan.class)
                .sinkTo(flightPlanSink).name("Flight plan sink");

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

        String encodedLoginJson = params.getRequired("loginJson");
        String decodedLoginJson = new String(Base64.getDecoder().decode(encodedLoginJson));

        // 1. 加载配置
        PersonnelAndPlanLoginConfig loginConfig = PersonnelAndPlanLoginConfig.builder()
                .loginUrl(params.getRequired("loginUrl"))
                .loginJson(decodedLoginJson)
                .build();
        FlightPlanReceiverConfig flightPlanReceiverConfig = FlightPlanReceiverConfig.builder()
                .flightDateUrl(params.getRequired("flightDateUrl"))
                .flightCodeUrl(params.getRequired("flightCodeUrl"))
                .flightXmlUrl(params.getRequired("flightXmlUrl"))
                .build();
        FlightPlanKafkaReceiver receiver = new FlightPlanKafkaReceiver(loginConfig, flightPlanReceiverConfig);
        receiver.run();
    }
}
