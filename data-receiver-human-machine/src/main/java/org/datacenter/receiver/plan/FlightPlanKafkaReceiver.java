package org.datacenter.receiver.plan;

import org.datacenter.agent.plan.FlightPlanAgent;
import org.datacenter.config.receiver.human.machine.plan.FlightPlanKafkaReceiverConfig;
import org.datacenter.config.sinker.human.machine.plan.FlightPlanSinkerConfig;
import org.datacenter.config.system.HumanMachineSysConfig;
import org.datacenter.receiver.BaseReceiver;

/**
 * @author : [wangminan]
 * @description : [一句话描述该类的功能]
 */
public class FlightPlanKafkaReceiver extends BaseReceiver {

    private final FlightPlanAgent flightPlanAgent;
    private final FlightPlanKafkaReceiverConfig receiverConfig;
    private final FlightPlanSinkerConfig sinkerConfig;

    public FlightPlanKafkaReceiver(FlightPlanKafkaReceiverConfig receiverConfig, FlightPlanSinkerConfig sinkerConfig) {
        // 1. 加载配置 HumanMachineSysConfig.loadConfig();
        HumanMachineSysConfig sysConfig = new HumanMachineSysConfig();
        sysConfig.loadConfig();
        this.flightPlanAgent = new FlightPlanAgent();
        this.receiverConfig = receiverConfig;
        this.sinkerConfig = sinkerConfig;
    }

    @Override
    public void prepare() {
        flightPlanAgent.run();
    }

    @Override
    public void start() {

    }
}
