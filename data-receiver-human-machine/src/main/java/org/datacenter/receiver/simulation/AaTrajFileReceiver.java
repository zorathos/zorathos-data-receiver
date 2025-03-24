package org.datacenter.receiver.simulation;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.jdbc.sink.JdbcSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.csv.CsvReaderFormat;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.function.SerializableFunction;
import org.apache.flink.util.function.SerializableSupplier;
import org.datacenter.config.simulation.SimulationReceiverConfig;
import org.datacenter.exception.ZorathosException;
import org.datacenter.model.base.TiDBDatabase;
import org.datacenter.model.base.TiDBTable;
import org.datacenter.model.simulation.AaTraj;
import org.datacenter.receiver.BaseReceiver;
import org.datacenter.receiver.util.DataReceiverUtil;
import org.datacenter.receiver.util.JdbcSinkUtil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Time;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import static org.datacenter.config.system.BaseSysConfig.humanMachineProperties;

/**
 * @author : [wangminan]
 * @description : 仿真数据AaTraj的接收器
 */
@Slf4j
@Data
@EqualsAndHashCode(callSuper = true)
public class AaTrajFileReceiver extends BaseReceiver {
    private SimulationReceiverConfig config;

    @Override
    public void prepare() {
        super.prepare();
        // 通过JDBC连接到AaTraj表 如果有和config.getSortieNumber()相同的记录就删除
        try {
            log.info("Linking to table: {}.{} for preparation.", TiDBDatabase.SIMULATION.getName(), TiDBTable.AA_TRAJ.getName());
            Class.forName(humanMachineProperties.getProperty("tidb.driverName"));
            Connection connection = DriverManager.getConnection(
                    JdbcSinkUtil.TIDB_URL_SIMULATION,
                    humanMachineProperties.getProperty("tidb.username"),
                    humanMachineProperties.getProperty("tidb.password"));
            String selectSql = """
                    SELECT COUNT(*) FROM `%s` WHERE `sortie_number` = ?;
                    """.formatted(TiDBTable.AA_TRAJ.getName());
            var preparedStatement = connection.prepareStatement(selectSql);
            preparedStatement.setString(1, config.getSortieNumber());
            var resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                int count = resultSet.getInt(1);
                if (count > 0) {
                    // 如果有对应记录 就删除
                    String deleteSql = """
                            DELETE FROM `%s` WHERE `sortie_number` = ?;
                            """.formatted(TiDBTable.AA_TRAJ.getName());
                    var deletePreparedStatement = connection.prepareStatement(deleteSql);
                    deletePreparedStatement.setString(1, config.getSortieNumber());
                    deletePreparedStatement.executeUpdate();
                    log.info("Delete {} records from table: {}.{}.", count, TiDBDatabase.SIMULATION.getName(), TiDBTable.AA_TRAJ.getName());
                } else {
                    // 如果没有对应记录 新建分区
                    String createPartitionSql = """
                            ALTER TABLE `%s` ADD PARTITION PARTITIONS 1;
                            """.formatted(TiDBTable.AA_TRAJ.getName());
                    var createPreparedStatement = connection.prepareStatement(createPartitionSql);
                    createPreparedStatement.executeUpdate();
                    log.info("Create partition for table: {}.{}.", TiDBDatabase.SIMULATION.getName(), TiDBTable.AA_TRAJ.getName());
                }
            }
            connection.close();
            log.info("Preparation finished.");
        } catch (SQLException | ClassNotFoundException e) {
            throw new ZorathosException(e, "Error occurs while preparing the AaTraj table.");
        }
    }

    @Override
    public void start() {
        StreamExecutionEnvironment env = DataReceiverUtil.prepareStreamEnv();
        // 列解析需要手动指定列顺序
        SerializableFunction<CsvMapper, CsvSchema> schemaGenerator = mapper -> CsvSchema.builder()
                .addColumn("aircraftId")
                .addColumn("messageTime")
                .addColumn("satelliteGuidanceTime")
                .addColumn("localTime")
                .addColumn("messageSequenceNumber")
                .addColumn("weaponId")
                .addColumn("pylonId")
                .addColumn("weaponType")
                .addColumn("targetId")
                .addColumn("longitude")
                .addColumn("latitude")
                .addColumn("altitude")
                .addColumn("missileTargetDistance")
                .addColumn("missileSpeed")
                .addColumn("interceptionStatus")
                .addColumn("nonInterceptionReason")
                .addColumn("seekerAzimuth")
                .addColumn("seekerElevation")
                .addColumn("targetTspiStatus")
                .addColumn("commandMachineStatus")
                .addColumn("groundAngleSatisfactionFlag")
                .addColumn("zeroCrossingFlag")
                .setUseHeader(true)
                .setColumnSeparator(',')
                .setLineSeparator("\n")
                .build();
        CsvReaderFormat<AaTraj> csvReaderFormat = CsvReaderFormat.forSchema(
                (SerializableSupplier<CsvMapper>) () -> {
                    CsvMapper csvMapper = new CsvMapper();
                    csvMapper.registerModule(new JavaTimeModule());
                    return csvMapper;
                },
                schemaGenerator,
                TypeInformation.of(AaTraj.class)
        );

        FileSource<AaTraj> fileSource = FileSource.forRecordStreamFormat(csvReaderFormat, new Path(config.getUrl())).build();
        String sortieNumber = config.getSortieNumber();
        Sink<AaTraj> sinkFunction = JdbcSink.<AaTraj>builder()
                .withQueryStatement("""
                                INSERT INTO `aa_traj` (
                                    sortie_number, aircraft_id, message_time, satellite_guidance_time, local_time, message_sequence_number, weapon_id, pylon_id, weapon_type, target_id, 
                                    longitude, latitude, altitude, missile_target_distance, missile_speed, interception_status, non_interception_reason, seeker_azimuth, seeker_elevation, 
                                    target_tspi_status, command_machine_status, ground_angle_satisfaction_flag, zero_crossing_flag
                                ) VALUES (
                                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                                );
                                """,
                        (preparedStatement, aaTraj) -> {

                            // 字符串转localDate sortieNumber.split("_")[0]
                            LocalDate localDate = LocalDate.parse(sortieNumber.split("_")[0], DateTimeFormatter.ofPattern("yyyyMMdd"));
                            // 注意 sortieNumber 是从配置里面来的 csv里面没有
                            preparedStatement.setString(1, sortieNumber);
                            preparedStatement.setString(2, aaTraj.getAircraftId());
                            // LocalTime解析不了 全部用Unix时间戳 Long型
                            preparedStatement.setTime(3, Time.valueOf(aaTraj.getMessageTime()));
                            preparedStatement.setTime(4, Time.valueOf(aaTraj.getSatelliteGuidanceTime()));
                            preparedStatement.setTime(5, Time.valueOf(aaTraj.getLocalTime()));
                            preparedStatement.setLong(6, aaTraj.getMessageSequenceNumber());
                            preparedStatement.setString(7, aaTraj.getWeaponId());
                            preparedStatement.setString(8, aaTraj.getPylonId());
                            preparedStatement.setString(9, aaTraj.getWeaponType());
                            preparedStatement.setString(10, aaTraj.getTargetId());
                            preparedStatement.setString(11, aaTraj.getLongitude());
                            preparedStatement.setString(12, aaTraj.getLatitude());
                            preparedStatement.setString(13, aaTraj.getAltitude());
                            preparedStatement.setString(14, aaTraj.getMissileTargetDistance());
                            preparedStatement.setString(15, aaTraj.getMissileSpeed());
                            preparedStatement.setString(16, aaTraj.getInterceptionStatus());
                            preparedStatement.setString(17, aaTraj.getNonInterceptionReason());
                            preparedStatement.setString(18, aaTraj.getSeekerAzimuth());
                            preparedStatement.setString(19, aaTraj.getSeekerElevation());
                            preparedStatement.setString(20, aaTraj.getTargetTspiStatus());
                            preparedStatement.setString(21, aaTraj.getCommandMachineStatus());
                            preparedStatement.setString(22, aaTraj.getGroundAngleSatisfactionFlag());
                            preparedStatement.setString(23, aaTraj.getZeroCrossingFlag());
                        })
                .withExecutionOptions(JdbcSinkUtil.getTiDBJdbcExecutionOptions())
                .buildAtLeastOnce(JdbcSinkUtil.getTiDBJdbcConnectionOptions(TiDBDatabase.SIMULATION));

        env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "AaTraj file source")
                .returns(AaTraj.class)
                .sinkTo(sinkFunction)
                .name("AaTraj File Sink");


        try {
            env.execute("AaTraj File Receiver");
        } catch (Exception e) {
            throw new ZorathosException(e, "Error occurs while executing the Flink job.");
        }
    }

    // 参数输入形式为 --url s3://human-machine/simulation/simulated_data_large.csv --sortie_number 20250303_五_01_ACT-3_邱陈_J16_07#02
    public static void main(String[] args) {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        SimulationReceiverConfig config = SimulationReceiverConfig.builder()
                .url(parameterTool.getRequired("url"))
                .sortieNumber(parameterTool.getRequired("sortie_number"))
                .build();
        AaTrajFileReceiver receiver = new AaTrajFileReceiver();
        receiver.setConfig(config);
        receiver.run();
    }
}
