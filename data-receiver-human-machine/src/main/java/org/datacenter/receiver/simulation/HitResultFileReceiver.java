package org.datacenter.receiver.simulation;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.flink.util.function.SerializableFunction;
import org.datacenter.config.receiver.simulation.SimulationReceiverConfig;
import org.datacenter.model.base.TiDBTable;
import org.datacenter.model.simulation.HitResult;
import org.datacenter.receiver.simulation.base.SimulationReceiver;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;


/**
 * @author : [宁]
 * @description : 仿真数据HitResult的接收器
 */
@Slf4j
@Data
@EqualsAndHashCode(callSuper = true)
public class HitResultFileReceiver extends SimulationReceiver<HitResult> {

    @Override
    public void prepare() {
        table = TiDBTable.HIT_RESULT;
        modelClass = HitResult.class;
        super.prepare();
    }

    @Override
    protected SerializableFunction<CsvMapper, CsvSchema> getSchemaGenerator() {
        return mapper -> CsvSchema.builder()
                // Although @JsonProperty uses Chinese, we map to the Java field names
                .addColumn("launcherId")  // 发射方ID
                .addColumn("targetId")    // 目标ID
                .addColumn("weaponType")  // 武器类型
                .addColumn("weaponId")    // 武器ID
                .addColumn("launchTime")  // 发射时间 (String in Java class)
                .addColumn("endTime")     // 结束时间 (LocalTime in Java class)
                .addColumn("hitResult")   // 命中结果
                .setUseHeader(true)
                .setColumnSeparator(',')
                .setLineSeparator("\n")
                .build();
    }

    @Override
    protected String getInsertQuery() {
        return """
                INSERT INTO `hit_result` (
                    sortie_number, launcher_id, target_id, weapon_type, weapon_id, launch_time, end_time, hit_result
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?
                );
                """;
    }

    @Override
    protected void bindPreparedStatement(PreparedStatement preparedStatement, HitResult data, String sortieNumber) throws SQLException {
        // 注意 sortieNumber 是从配置里面来的 csv里面没有
        preparedStatement.setString(1, sortieNumber);
        preparedStatement.setString(2, data.getLauncherId());
        preparedStatement.setString(3, data.getTargetId());
        preparedStatement.setString(4, data.getWeaponType());
        preparedStatement.setString(5, data.getWeaponId());
        preparedStatement.setString(6, data.getLaunchTime()); // launchTime is String
        // LocalTime -> java.sql.Time
        preparedStatement.setTime(7, data.getEndTime() != null ? Time.valueOf(data.getEndTime()) : null);
        preparedStatement.setString(8, data.getHitResult());
    }

    @Override
    public void start() {
        super.start();
    }

    // 参数输入形式为 --url s3://human-machine/simulation/simulated_data_large.csv --sortie_number 20250303_五_01_ACT-3_邱陈_J16_07#02
    public static void main(String[] args) {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        SimulationReceiverConfig config = new SimulationReceiverConfig(
                parameterTool.getRequired("url"),
                parameterTool.getRequired("sortie_number"));
        HitResultFileReceiver receiver = new HitResultFileReceiver();
        receiver.setConfig(config);
        receiver.run();
    }
}
