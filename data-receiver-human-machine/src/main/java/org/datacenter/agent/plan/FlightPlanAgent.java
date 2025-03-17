package org.datacenter.agent.plan;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.datacenter.agent.BaseAgent;
import org.datacenter.agent.util.KafkaUtil;
import org.datacenter.agent.util.PersonnelAndFlightPlanHttpClientUtil;
import org.datacenter.config.plan.FlightPlanReceiverConfig;
import org.datacenter.exception.ZorathosException;
import org.datacenter.model.plan.FlightPlanRoot;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.datacenter.config.system.BaseSysConfig.humanMachineProperties;

/**
 * @author : [wangminan]
 * @description : 通过飞行计划接口获取飞行计划数据 投递到Kafka
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Slf4j
@AllArgsConstructor
public class FlightPlanAgent extends BaseAgent {

    private final ObjectMapper mapper;
    private final FlightPlanReceiverConfig receiverConfig;
    private ScheduledExecutorService scheduler;

    public FlightPlanAgent(FlightPlanReceiverConfig receiverConfig) {
        super();
        this.receiverConfig = receiverConfig;
        this.mapper = new ObjectMapper();
    }

    @Override
    public void run() {
        super.run();
        log.info("Flight plan agent start running, fetching data from flight agent system's xml interface and sending it to kafka.");

        if (!isStartedByThisInstance) {
            return;
        }

        if (scheduler == null) {
            scheduler = Executors.newScheduledThreadPool(1);
        }

        scheduler.scheduleAtFixedRate(() -> {
            if (prepared) {
                // 这玩意没有主键 所以在每一次写入之前都需要清空所有原有数据
                // 1. 清空原有库表数据 用jdbc
                truncateFlightPlanTables();
                // 然后才可以拉Flink
                running = true;
                // 1. 获取飞行计划根XML并解析
                List<FlightPlanRoot> flightPlans = PersonnelAndFlightPlanHttpClientUtil.getFlightRoots(receiverConfig);
                // 2. 转发到Kafka
                try {
                    String plansInJson = mapper.writeValueAsString(flightPlans);
                    KafkaUtil.sendMessage(humanMachineProperties
                            .getProperty("kafka.topic.flightPlanRoot"), plansInJson);
                } catch (JsonProcessingException e) {
                    throw new ZorathosException(e, "Error occurs while converting flight plans to json string.");
                }
            }
        }, 0, Integer.parseInt(humanMachineProperties.getProperty("agent.interval.flightPlan")), TimeUnit.MINUTES);
    }

    private void truncateFlightPlanTables() {
        try {
            log.info("Start truncating personnel info table.");
            Class.forName(humanMachineProperties.getProperty("tidb.driverName"));
            Connection connection = DriverManager.getConnection(
                    humanMachineProperties.getProperty("tidb.url.humanMachine"),
                    humanMachineProperties.getProperty("tidb.username"),
                    humanMachineProperties.getProperty("tidb.password"));
            connection.prepareStatement("TRUNCATE TABLE `human_machine`.`flight_cmd`;").execute();
            connection.prepareStatement("TRUNCATE TABLE `human_machine`.`flight_head`;").execute();
            connection.prepareStatement("TRUNCATE TABLE `human_machine`.`flight_notes`;").execute();
            connection.prepareStatement("TRUNCATE TABLE `human_machine`.`flight_plan`;").execute();
            connection.prepareStatement("TRUNCATE TABLE `human_machine`.`flight_plan_root`;").execute();
            connection.prepareStatement("TRUNCATE TABLE `human_machine`.`flight_task`;").execute();
            connection.close();
            log.info("Truncate personnel info table successfully.");
        } catch (SQLException | ClassNotFoundException e) {
            throw new ZorathosException(e, "Error occurs while truncating personnel database.");
        }
    }

    @Override
    public void stop() {
        super.stop();
        scheduler.shutdown();
    }
}
