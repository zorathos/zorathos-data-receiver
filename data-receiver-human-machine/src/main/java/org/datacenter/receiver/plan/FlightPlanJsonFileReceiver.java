package org.datacenter.receiver.plan;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.datacenter.config.HumanMachineConfig;
import org.datacenter.config.receiver.plan.FlightPlanJsonFileReceiverConfig;
import org.datacenter.exception.ZorathosException;
import org.datacenter.model.plan.FlightPlanRoot;
import org.datacenter.model.plan.response.FlightPlanResponse;
import org.datacenter.receiver.BaseReceiver;
import org.datacenter.receiver.plan.util.FlightPlanSinkUtil;
import org.datacenter.receiver.util.DataReceiverUtil;
import org.datacenter.receiver.util.JsonFileSingleInputFormat;

import java.util.UUID;

/**
 * @author : [wangminan]
 * @description : 飞行计划JSON接收器
 */
@Slf4j
public class FlightPlanJsonFileReceiver extends BaseReceiver {

    private final FlightPlanJsonFileReceiverConfig config;

    public FlightPlanJsonFileReceiver(FlightPlanJsonFileReceiverConfig config) {
        HumanMachineConfig humanMachineConfig = new HumanMachineConfig();
        humanMachineConfig.loadConfig();
        this.config = config;
    }

    @Override
    public void prepare() {
        super.prepare();
    }

    @Override
    public void start() {
        StreamExecutionEnvironment env = DataReceiverUtil.prepareStreamEnv();
        JsonFileSingleInputFormat<FlightPlanResponse> inputFormat = new JsonFileSingleInputFormat<>(FlightPlanResponse.class);

        FileSource<FlightPlanResponse> source = FileSource
                .forRecordStreamFormat(inputFormat, new Path(config.getUrl()))
                .build();

        DataStreamSource<FlightPlanResponse> flightPlanSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "FlightPlanSource");
        SingleOutputStreamOperator<FlightPlanRoot> sourceDs = flightPlanSource
                .map(flightPlanResponse -> {
                    String xml = flightPlanResponse.getXml();
                    String rootId;
                    if (flightPlanResponse.getPlanRootId() != null) {
                        rootId = flightPlanResponse.getPlanRootId();
                    } else {
                        rootId = UUID.randomUUID().toString();
                    }
                    FlightPlanRoot flightPlanRoot = FlightPlanRoot.fromXml(xml, rootId);
                    flightPlanRoot.setFlightDate(flightPlanResponse.getFlightDate());
                    return flightPlanRoot;
                })
                .returns(FlightPlanRoot.class);

        // 重复使用datastream flink在每一次对datastream操作之后都会new一个新的对象 所以不用担心反复消费的问题
        FlightPlanSinkUtil.addMultiSinkForFlightPlanRoot(sourceDs);

        try {
            env.execute();
        } catch (Exception e) {
            throw new ZorathosException(e, "Encounter error when sinking flight plan data to tidb.");
        }
    }

    public static void main(String[] args) {
        ParameterTool params = ParameterTool.fromArgs(args);
        log.info("Parameters: {}", params.toMap());

        FlightPlanJsonFileReceiverConfig config = FlightPlanJsonFileReceiverConfig.builder()
                .url(params.getRequired("url"))
                .build();

        FlightPlanJsonFileReceiver receiver = new FlightPlanJsonFileReceiver(config);
        receiver.run();
    }
}
