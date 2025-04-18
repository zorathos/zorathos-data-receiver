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
import org.datacenter.model.simulation.Sendto3DData;
import org.datacenter.receiver.simulation.base.SimulationReceiver;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import static org.datacenter.config.keys.HumanMachineReceiverConfigKey.SIMULATION_SORTIE_NUMBER;
import static org.datacenter.config.keys.HumanMachineReceiverConfigKey.SIMULATION_URL;


/**
 * @author : [宁]
 * @description : 仿真数据Sendto3DData的接收器
 */
@Slf4j
@Data
@EqualsAndHashCode(callSuper = true)
public class Sendto3DDataFileReceiver extends SimulationReceiver<Sendto3DData> {

    @Override
    public void prepare() {
        table = TiDBTable.SENDTO_3D_DATA;
        modelClass = Sendto3DData.class;
        super.prepare();
    }

    @Override
    protected SerializableFunction<CsvMapper, CsvSchema> getSchemaGenerator() {
        return mapper -> CsvSchema.builder()
                .addColumn("aircraftId")         // 飞机ID
                .addColumn("aircraftCallsign")   // 飞机代字
                .addColumn("aircraftCodeName")   // 飞机代号
                .addColumn("redBlueAffiliation") // 红蓝属性
                .addColumn("flightBatch")        // 飞行批次
                .setUseHeader(true)
                .setColumnSeparator(',')
                .setLineSeparator("\n")
                .build();
    }

    @Override
    protected String getInsertQuery() {
        return """
                INSERT INTO `sendto_3d_data` (
                    sortie_number, aircraft_id, aircraft_callsign, aircraft_code_name, red_blue_affiliation, flight_batch
                ) VALUES (
                    ?, ?, ?, ?, ?, ?
                );
                """;
    }

    @Override
    protected void bindPreparedStatement(PreparedStatement preparedStatement, Sendto3DData data, String sortieNumber) throws SQLException {
        // 注意 sortieNumber 是从配置里面来的 csv里面没有
        preparedStatement.setString(1, sortieNumber);
        preparedStatement.setString(2, data.getAircraftId());
        preparedStatement.setString(3, data.getAircraftCallsign());
        preparedStatement.setString(4, data.getAircraftCodeName());
        preparedStatement.setString(5, data.getRedBlueAffiliation());
        preparedStatement.setString(6, data.getFlightBatch());
    }

    @Override
    public void start() {
        super.start();
    }

    // 参数输入形式为 --url s3://human-machine/simulation/simulated_data_large.csv --sortie_number 20250303_五_01_ACT-3_邱陈_J16_07#02
    public static void main(String[] args) {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        SimulationReceiverConfig config = new SimulationReceiverConfig(
                parameterTool.getRequired(SIMULATION_URL.getKeyForParamsMap()),
                parameterTool.getRequired(SIMULATION_SORTIE_NUMBER.getKeyForParamsMap()));
        Sendto3DDataFileReceiver receiver = new Sendto3DDataFileReceiver();
        receiver.setConfig(config);
        receiver.run();
    }
}
