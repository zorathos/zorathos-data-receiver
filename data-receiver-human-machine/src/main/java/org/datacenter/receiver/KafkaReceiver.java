package org.datacenter.receiver;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.datacenter.config.receiver.BaseKafkaReceiverConfig;
import org.datacenter.exception.ZorathosException;
import org.datacenter.receiver.util.DataReceiverUtil;

import java.io.Serial;
import java.io.Serializable;
import java.util.List;

/**
 * @author : [wangminan]
 * @description : Kafka接收器
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Slf4j
public abstract class KafkaReceiver <T, C extends BaseKafkaReceiverConfig> extends BaseReceiver implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;
    protected C config;
    protected Boolean itemInArray = false;
    protected transient Class<T> modelClass;

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

        SingleOutputStreamOperator<T> kafkaSourceDS;

        if (itemInArray) {
            kafkaSourceDS = DataReceiverUtil.getKafkaArraySourceDS(env, List.of(config.getTopic()), modelClass);
        } else {
            // 实际返回的是一个DataStreamSource 能用到并行化特性
            kafkaSourceDS = DataReceiverUtil.getKafkaSourceDS(env, List.of(config.getTopic()), modelClass);
        }

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
}
