package org.datacenter.agent.plan;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.datacenter.agent.BaseAgent;
import org.datacenter.config.plan.FlightPlanKafkaReceiverConfig;

/**
 * @author : [wangminan]
 * @description : 通过飞行计划接口获取飞行计划数据 投递到Kafka
 */
@EqualsAndHashCode(callSuper = true)
@Data
@NoArgsConstructor
@AllArgsConstructor
@Slf4j
public class FlightPlanAgent extends BaseAgent {

    private FlightPlanKafkaReceiverConfig config;

    @Override
    public void run() {
        log.info("Flight plan agent start running, fetching data from flight agent system's xml interface and sending it to kafka.");

        while(running) {
            // 1. 登录并获取Cookie
            // 2. 发送请求
            // 3. 获取数据并转发到Kafka
        }
    }
}
