package org.datacenter.receiver.physiological;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.datacenter.config.physiological.PhysiologicalKafkaReceiverConfig;
import org.datacenter.receiver.BaseReceiver;

/**
 * @author : [wangminan]
 * @description : 眼动数据接收
 */
@Slf4j
@AllArgsConstructor
public class EyeMovementKafkaReceiver extends BaseReceiver {

    private PhysiologicalKafkaReceiverConfig config;

    @Override
    public void prepare() {
        super.prepare();
    }

    @Override
    public void start() {

    }

    /**
     * 主函数
     * @param args 接收器参数 入参为 --topic xxxx
     */
    public static void main(String[] args) {
        ParameterTool params = ParameterTool.fromArgs(args);
        String topic = params.getRequired("topic");
        PhysiologicalKafkaReceiverConfig config = PhysiologicalKafkaReceiverConfig.builder()
                .topic(topic)
                .build();
        EyeMovementKafkaReceiver receiver = new EyeMovementKafkaReceiver(config);
        receiver.run();
    }
}
