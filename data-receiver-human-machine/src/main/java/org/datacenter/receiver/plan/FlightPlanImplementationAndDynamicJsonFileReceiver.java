package org.datacenter.receiver.plan;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.datacenter.config.receiver.plan.FlightPlanImplementationAndDynamicJsonFileReceiverConfig;
import org.datacenter.exception.ZorathosException;
import org.datacenter.model.base.TiDBDatabase;
import org.datacenter.model.plan.FlightPlanRoot;
import org.datacenter.model.plan.response.FlightPlanImplementationAndDynamicResponseSingleton;
import org.datacenter.receiver.BaseReceiver;
import org.datacenter.receiver.plan.util.FlightPlanSinkUtil;
import org.datacenter.receiver.util.DataReceiverUtil;
import org.datacenter.receiver.util.JsonArrayFileInputFormat;

import java.util.UUID;

/**
 * @author : [wangminan]
 * @description : 飞行计划实施和动态接收器
 */
@Slf4j
public class FlightPlanImplementationAndDynamicJsonFileReceiver extends BaseReceiver {

    private final FlightPlanImplementationAndDynamicJsonFileReceiverConfig receiverConfig;

    public  FlightPlanImplementationAndDynamicJsonFileReceiver(FlightPlanImplementationAndDynamicJsonFileReceiverConfig receiverConfig) {
        this.receiverConfig = receiverConfig;
    }

    @Override
    public void prepare() {
        super.prepare();
    }

    @Override
    public void start() {
        StreamExecutionEnvironment env = DataReceiverUtil.prepareStreamEnv();
        JsonArrayFileInputFormat<FlightPlanImplementationAndDynamicResponseSingleton> inputFormat = new JsonArrayFileInputFormat<>(FlightPlanImplementationAndDynamicResponseSingleton.class);

        FileSource<FlightPlanImplementationAndDynamicResponseSingleton> source = FileSource
                .forRecordStreamFormat(inputFormat, new Path(receiverConfig.getUrl()))
                .build();

        DataStreamSource<FlightPlanImplementationAndDynamicResponseSingleton> flightPlanSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "FlightPlanSource");
        SingleOutputStreamOperator<FlightPlanRoot> sourceDs = flightPlanSource
                .map(responseSingleton -> {
                    String xml = responseSingleton.getXml();
                    String rootId;
                    if (responseSingleton.getPlanRootId() != null) {
                        rootId = responseSingleton.getPlanRootId();
                    } else {
                        rootId = UUID.randomUUID().toString();
                    }
                    FlightPlanRoot flightPlanRoot = FlightPlanRoot.fromXml(xml, rootId);
                    flightPlanRoot.setFlightDate(responseSingleton.getFlightDateTime().toLocalDate());
                    flightPlanRoot.setFlightDateTime(responseSingleton.getFlightDateTime());
                    return flightPlanRoot;
                })
                .returns(FlightPlanRoot.class);

        if (this.receiverConfig.getReceiverType().equals(FlightPlanImplementationAndDynamicJsonFileReceiverConfig.FlightPlanReceiverType.DYNAMIC)) {
            // 重复使用datastream flink在每一次对datastream操作之后都会new一个新的对象 所以不用担心反复消费的问题
            FlightPlanSinkUtil.addMultiSinkForFlightPlanRoot(sourceDs, TiDBDatabase.FLIGHT_PLAN_DYNAMIC);
        } else if (this.receiverConfig.getReceiverType().equals(FlightPlanImplementationAndDynamicJsonFileReceiverConfig.FlightPlanReceiverType.IMPLEMENTATION)) {
            FlightPlanSinkUtil.addMultiSinkForFlightPlanRoot(sourceDs, TiDBDatabase.FLIGHT_PLAN_IMPLEMENTATION);
        } else {
            throw new ZorathosException("Receiver type not supported.");
        }

        try {
            env.execute();
        } catch (Exception e) {
            throw new ZorathosException(e, "Encounter error when sinking flight plan data to tidb.");
        }
    }

    public static void main(String[] args) {
        ParameterTool params = ParameterTool.fromArgs(args);
        log.info("Parameters: {}", params.toMap());

        FlightPlanImplementationAndDynamicJsonFileReceiverConfig config = FlightPlanImplementationAndDynamicJsonFileReceiverConfig.builder()
                .receiverType(FlightPlanImplementationAndDynamicJsonFileReceiverConfig.FlightPlanReceiverType.fromString(params.getRequired("receiverType")))
                .url(params.getRequired("url"))
                .build();

        FlightPlanImplementationAndDynamicJsonFileReceiver receiver = new FlightPlanImplementationAndDynamicJsonFileReceiver(config);
        receiver.start();
    }
}
