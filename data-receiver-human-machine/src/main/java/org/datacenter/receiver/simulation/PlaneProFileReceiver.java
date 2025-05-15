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
import org.datacenter.model.simulation.PlanePro;
import org.datacenter.receiver.simulation.base.SimulationReceiver;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import static org.datacenter.config.keys.HumanMachineReceiverConfigKey.IMPORT_ID;
import static org.datacenter.config.keys.HumanMachineReceiverConfigKey.SIMULATION_BATCH_NUMBER;
import static org.datacenter.config.keys.HumanMachineReceiverConfigKey.SIMULATION_URL;


/**
 * @author : [宁]
 * @description : 仿真数据PlanePro的接收器
 */
@Slf4j
@Data
@EqualsAndHashCode(callSuper = true)
public class PlaneProFileReceiver extends SimulationReceiver<PlanePro> {

    @Override
    public void prepare() {
        table = TiDBTable.PLANE_PRO;
        modelClass = PlanePro.class;
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
                INSERT INTO `plane_pro` (
                    import_id,batch_number, aircraft_id, aircraft_callsign, aircraft_code_name, red_blue_affiliation, flight_batch
                ) VALUES (
                    ?,?, ?, ?, ?, ?, ?
                );
                """;
    }

    @Override
    protected void bindPreparedStatement(PreparedStatement preparedStatement, PlanePro data, String batchNumber, long importId) throws SQLException {
        preparedStatement.setLong(1, importId);        preparedStatement.setString(2, batchNumber);
        preparedStatement.setString(3, data.getAircraftId());
        preparedStatement.setString(4, data.getAircraftCallsign());
        preparedStatement.setString(5, data.getAircraftCodeName());
        preparedStatement.setString(6, data.getRedBlueAffiliation());
        preparedStatement.setString(7, data.getFlightBatch());
    }

    @Override
    public void start() {
        super.start();
    }

    // 参数输入形式为 --url s3://human-machine/simulation/simulated_data_large.csv --import_id 12345 --batch_number 20250303_五_01_ACT-3_邱陈_J16_07#02
    public static void main(String[] args) {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        SimulationReceiverConfig config = new SimulationReceiverConfig(
                parameterTool.getRequired(SIMULATION_URL.getKeyForParamsMap()),
                Long.parseLong(parameterTool.getRequired(IMPORT_ID.getKeyForParamsMap())),
                parameterTool.getRequired(SIMULATION_BATCH_NUMBER.getKeyForParamsMap()));
        PlaneProFileReceiver receiver = new PlaneProFileReceiver();
        receiver.setConfig(config);
        receiver.run();
    }
}
