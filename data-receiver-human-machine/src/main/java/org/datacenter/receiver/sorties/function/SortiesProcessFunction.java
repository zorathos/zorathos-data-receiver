package org.datacenter.receiver.sorties.function;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.datacenter.config.receiver.sorties.SortiesReceiverConfig;
import org.datacenter.model.sorties.Sorties;

import java.io.Serial;
import java.io.Serializable;

/**
 * @author : [wangminan]
 * @description : [Sorties数据处理函数]
 */
@Slf4j
public class SortiesProcessFunction extends KeyedProcessFunction<String, Sorties, Sorties> implements Serializable {
    @Serial
    private static final long serialVersionUID = 5608907463L;

    // 只存储必要的字段，而不是整个配置对象
    private final SortiesReceiverConfig.RunMode runMode;
    private final Long stopSignalDuration;

    public SortiesProcessFunction(SortiesReceiverConfig.RunMode runMode, Long stopSignalDuration) {
        this.runMode = runMode;
        this.stopSignalDuration = stopSignalDuration;
    }

    @Override
    public void processElement(Sorties sorties, KeyedProcessFunction<String, Sorties, Sorties>.Context context, Collector<Sorties> collector) {
        // 使用runMode代替receiverConfig.getRunMode()
        if (runMode.equals(SortiesReceiverConfig.RunMode.AT_ONCE)
                && sorties.getSortieNumber() != null
                && sorties.getSortieNumber().startsWith(Sorties.END_SIGNAL_PREFIX)) {
            log.info("Received end signal: {}, receiver will shutdown after 30 seconds after all data saved.", sorties.getSortieNumber());
            context.timerService().registerProcessingTimeTimer(System.currentTimeMillis() + stopSignalDuration);
        } else {
            collector.collect(sorties);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Sorties> out) throws Exception {
        super.onTimer(timestamp, ctx, out);
        log.info("Received end signal, shutting down agent and receiver.");
        System.exit(0);
    }
}
