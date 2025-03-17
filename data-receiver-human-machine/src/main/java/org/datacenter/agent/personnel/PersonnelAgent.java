package org.datacenter.agent.personnel;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.datacenter.agent.BaseAgent;
import org.datacenter.agent.util.KafkaUtil;
import org.datacenter.agent.util.PersonnelAndFlightPlanHttpClientUtil;
import org.datacenter.config.personnel.PersonnelReceiverConfig;
import org.datacenter.exception.ZorathosException;
import org.datacenter.model.crew.PersonnelInfo;

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
 * @description : 人员信息拉取Agent
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Slf4j
@AllArgsConstructor
public class PersonnelAgent extends BaseAgent {

    private final ObjectMapper mapper;
    private final PersonnelReceiverConfig receiverConfig;
    private ScheduledExecutorService scheduler;

    public PersonnelAgent(PersonnelReceiverConfig receiverConfig) {
        super();
        this.receiverConfig = receiverConfig;
        this.mapper = new ObjectMapper();
    }

    @Override
    public void run() {
        super.run();
        log.info("Personnel agent start running, fetching data from personnel agent system's json interface and sending it to kafka.");

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
                truncatePersonnelInfoTable();
                // 这时候才可以拉起Flink任务
                running = true;
                // 2. 拉取人员数据
                List<PersonnelInfo> personnelInfos = PersonnelAndFlightPlanHttpClientUtil.getPersonnelInfos(receiverConfig);
                // 3. 转发到Kafka
                try {
                    String personnelInfosInJson = mapper.writeValueAsString(personnelInfos);
                    KafkaUtil.sendMessage(humanMachineProperties
                            .getProperty("kafka.topic.personnel"), personnelInfosInJson);
                } catch (JsonProcessingException e) {
                    throw new ZorathosException(e, "Error occurs while converting personnel infos to json string.");
                }
            }
        },
        0, Integer.parseInt(humanMachineProperties.getProperty("agent.interval.personnel")), TimeUnit.MINUTES);
    }

    private void truncatePersonnelInfoTable() {
        try {
            log.info("Start truncating personnel info table.");
            Class.forName(humanMachineProperties.getProperty("tidb.driverName"));
            Connection connection = DriverManager.getConnection(
                    humanMachineProperties.getProperty("tidb.url.humanMachine"),
                    humanMachineProperties.getProperty("tidb.username"),
                    humanMachineProperties.getProperty("tidb.password"));
            connection.prepareStatement("TRUNCATE TABLE `personnel_info`;").execute();
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
