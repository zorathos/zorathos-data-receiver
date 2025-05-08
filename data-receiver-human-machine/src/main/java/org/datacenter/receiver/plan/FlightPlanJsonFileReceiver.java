package org.datacenter.receiver.plan;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.datacenter.config.receiver.plan.FlightPlanJsonFileReceiverConfig;
import org.datacenter.exception.ZorathosException;
import org.datacenter.model.base.TiDBDatabase;
import org.datacenter.model.plan.FlightPlanRoot;
import org.datacenter.receiver.BaseReceiver;
import org.datacenter.receiver.plan.util.FlightPlanSinkUtil;
import org.datacenter.receiver.util.DataReceiverUtil;
import org.datacenter.receiver.util.JsonArrayFileInputFormat;

import static org.datacenter.config.keys.HumanMachineReceiverConfigKey.FLIGHT_PLAN_FILE_URL;

/**
 * @author : [wangminan]
 * @description : 飞行计划JSON接收器
 */
@Slf4j
public class FlightPlanJsonFileReceiver extends BaseReceiver {

    private final FlightPlanJsonFileReceiverConfig config;

    public FlightPlanJsonFileReceiver(FlightPlanJsonFileReceiverConfig config) {
        this.config = config;
    }

    @Override
    public void prepare() {
        super.prepare();
    }

    @Override
    public void start() {
        StreamExecutionEnvironment env = DataReceiverUtil.prepareStreamEnv();
        JsonArrayFileInputFormat<FlightPlanRoot> inputFormat = new JsonArrayFileInputFormat<>(FlightPlanRoot.class);

        FileSource<FlightPlanRoot> source = FileSource
                .forRecordStreamFormat(inputFormat, new Path(config.getUrl()))
                .build();

        DataStreamSource<FlightPlanRoot> flightPlanSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "FlightPlanSource");

        // 重复使用datastream flink在每一次对datastream操作之后都会new一个新的对象 所以不用担心反复消费的问题
        FlightPlanSinkUtil.addMultiSinkForFlightPlanRoot(flightPlanSource, TiDBDatabase.FLIGHT_PLAN);

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
                .url(params.getRequired(FLIGHT_PLAN_FILE_URL.getKeyForParamsMap()))
                .build();

        FlightPlanJsonFileReceiver receiver = new FlightPlanJsonFileReceiver(config);
        receiver.run();
    }
}
