package org.datacenter.receiver.plan;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.datacenter.agent.plan.FlightPlanAgent;
import org.datacenter.config.plan.FlightPlanReceiverConfig;
import org.datacenter.config.system.HumanMachineSysConfig;
import org.datacenter.exception.ZorathosException;
import org.datacenter.model.plan.FlightPlan;
import org.datacenter.receiver.BaseReceiver;
import org.datacenter.receiver.util.DataReceiverUtil;

import java.util.List;

import static org.datacenter.config.system.BaseSysConfig.humanMachineProperties;

/**
 * @author : [wangminan]
 * @description : [一句话描述该类的功能]
 */
@SuppressWarnings("deprecation")
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
        // 开始从kafka获取数据
        // 引入执行环境
        StreamExecutionEnvironment env = DataReceiverUtil.prepareStreamEnv();
        DataStreamSource<FlightPlan> kafkaSourceDS =
                DataReceiverUtil.getKafkaSourceDS(env, List.of(humanMachineProperties.getProperty("kafka.topic.flightPlan")), FlightPlan.class);
        // 投递到数据库
        SinkFunction<FlightPlan> sinkFunction = JdbcSink.sink(
                    """
                    INSERT INTO `flight_plan` (
                        airport_id, takeoff_time, air_battle_time, ys, outline, lx, cs, sj, jhlx, plan_time, height, ky, fz, bdno,
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
                    preparedStatement.setString(6, flightPlan.getLx());
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
                JdbcExecutionOptions.builder()
                        .withBatchSize(Integer.parseInt(humanMachineProperties.getProperty("flink.jdbc.sinker.batchSize")))
                        .withBatchIntervalMs(Integer.parseInt(humanMachineProperties.getProperty("flink.jdbc.sinker.batchInterval")))
                        .withMaxRetries(Integer.parseInt(humanMachineProperties.getProperty("flink.jdbc.sinker.maxRetries")))
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(humanMachineProperties.getProperty("tidb.url.humanMachine"))
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
