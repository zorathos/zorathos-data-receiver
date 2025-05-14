package org.datacenter.receiver.physiological.base;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.datacenter.config.receiver.physiological.PhysiologicalKafkaReceiverConfig;
import org.datacenter.exception.ZorathosException;
import org.datacenter.receiver.BaseReceiver;
import org.datacenter.receiver.util.DataReceiverUtil;

import java.util.List;

import static org.datacenter.config.keys.HumanMachineReceiverConfigKey.IMPORT_ID;
import static org.datacenter.config.keys.HumanMachineReceiverConfigKey.PHYSIOLOGY_KAFKA_TOPIC;

/**
 * @author : [wangminan]
 * @description : 生理数据Kafka接收器基类
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Slf4j
public abstract class PhysiologicalKafkaReceiver<T> extends BaseReceiver {
    protected PhysiologicalKafkaReceiverConfig config;

    /**
     * 创建特定的Sink
     *
     * @param importId 导入ID
     * @return 创建的Sink
     */
    protected abstract Sink<T> createSink(Long importId);

    /**
     * 获取接收器名称
     *
     * @return 接收器名称
     */
    protected abstract String getReceiverName();

    /**
     * 获取接收的数据类型Class
     *
     * @return 数据类型Class
     */
    protected abstract Class<T> getDataClass();

    @Override
    public void prepare() {
        super.prepare();
    }

    @Override
    public void start() {
        // 获取导入ID，避免将整个config传入createSink方法，防止序列化问题
        Long importId = config.getImportId();
        String receiverName = getReceiverName();

        StreamExecutionEnvironment env = DataReceiverUtil.prepareStreamEnv();
        env.setParallelism(1);

        DataStreamSource<T> kafkaSourceDS =
                DataReceiverUtil.getKafkaSourceDS(env, List.of(config.getTopic()), getDataClass());

        // 创建sink
        Sink<T> sink = createSink(importId);

        kafkaSourceDS
                .sinkTo(sink).name(receiverName + " Kafka Sinker");

        try {
            env.execute(receiverName + " Kafka Receiver");
        } catch (Exception e) {
            throw new ZorathosException(e, "Encounter error when executing " + receiverName + "KafkaReceiver.");
        }
    }

    /**
     * 解析命令行参数
     */
    protected static PhysiologicalKafkaReceiverConfig parseArgs(String[] args) {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        return PhysiologicalKafkaReceiverConfig.builder()
                .importId(Long.valueOf(parameterTool.getRequired(IMPORT_ID.getKeyForParamsMap())))
                .topic(parameterTool.getRequired(PHYSIOLOGY_KAFKA_TOPIC.getKeyForParamsMap()))
                .build();
    }
}
