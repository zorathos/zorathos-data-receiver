package org.datacenter.receiver.sorties.function;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.datacenter.config.receiver.sorties.SortiesBatchReceiverConfig;
import org.datacenter.model.sorties.Sorties;
import org.datacenter.model.sorties.SortiesBatch;

import java.io.Serial;

/**
 * @author : [wangminan]
 * @description : [一句话描述该类的功能]
 */
@Slf4j
public class SortiesBatchProcessFunction extends KeyedProcessFunction<String, SortiesBatch, SortiesBatch> {
    @Serial
    private static final long serialVersionUID = 12213147L;
    // 只存储必要的字段，而不是整个配置对象
    private final SortiesBatchReceiverConfig.RunMode runMode;
    private final Long stopSignalDuration;

    public SortiesBatchProcessFunction(SortiesBatchReceiverConfig.RunMode runMode, Long stopSignalDuration) {
        this.runMode = runMode;
        this.stopSignalDuration = stopSignalDuration;
    }

    @Override
    public void processElement(SortiesBatch sortiesBatch, KeyedProcessFunction<String, SortiesBatch, SortiesBatch>.Context context, Collector<SortiesBatch> collector) {
        // 添加单次运行时处理逻辑
        if (runMode.equals(SortiesBatchReceiverConfig.RunMode.AT_ONCE)
                && sortiesBatch.getBatchNumber() != null
                && sortiesBatch.getBatchNumber().startsWith(Sorties.END_SIGNAL_PREFIX)) {
            log.info("Received end signal: {}, receiver will shutdown after 30 seconds after all data saved.", sortiesBatch.getBatchNumber());
            context.timerService().registerProcessingTimeTimer(System.currentTimeMillis() + stopSignalDuration);
        } else {
            // 正常数据流转
            collector.collect(sortiesBatch);
        }
    }

    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<String, SortiesBatch, SortiesBatch>.OnTimerContext ctx, Collector<SortiesBatch> out) throws Exception {
        super.onTimer(timestamp, ctx, out);
        log.info("Received end signal, shutting down agent and receiver.");
        System.exit(0);
    }

}
