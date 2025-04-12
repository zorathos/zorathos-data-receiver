package org.datacenter.test;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.csv.CsvReaderFormat;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.function.SerializableFunction;
import org.apache.flink.util.function.SerializableSupplier;
import org.datacenter.config.HumanMachineConfig;
import org.datacenter.exception.ZorathosException;
import org.datacenter.model.base.TiDBDatabase;
import org.datacenter.model.simulation.AaTraj;
import org.datacenter.receiver.util.DataReceiverUtil;
import org.datacenter.receiver.util.JavaTimeUtil;
import org.datacenter.receiver.util.JdbcSinkUtil;

import java.sql.PreparedStatement;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

@Slf4j
public class AaTrajTest {

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class SimpleSortie {
        private String sortieNumber;

        /**
         * 消息时间
         */
        @JsonFormat(pattern = "HH:mm:ss.SSS", timezone = "GMT+8")
        private LocalTime messageTime;

        /**
         * 卫导时间
         */
        @JsonFormat(pattern = "HH:mm:ss.SSS", timezone = "GMT+8")
        private LocalTime satelliteGuidanceTime;

        /**
         * 本地时间
         */
        @JsonFormat(pattern = "HH:mm:ss.SSS", timezone = "GMT+8")
        private LocalTime localTime;
    }

    public static void main(String[] args) {
        HumanMachineConfig humanMachineSysConfig = new HumanMachineConfig();
        humanMachineSysConfig.loadConfig();

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

        // 定义 FileSource
        FileSource<AaTraj> fileSource = FileSource.forRecordStreamFormat(
                csvReaderFormat,
                new Path("D:\\onedrive\\桌面\\simulated_data_large.csv")
        ).build();

        // 定义 SinkFunction
        SinkFunction<AaTraj> sinkFunction = JdbcSink.sink(
                """
                        INSERT INTO `aa_traj` (
                            sortie_number, aircraft_id, message_time, satellite_guidance_time, local_time, message_sequence_number, weapon_id, pylon_id, weapon_type, target_id, 
                            longitude, latitude, altitude, missile_target_distance, missile_speed, interception_status, non_interception_reason, seeker_azimuth, seeker_elevation, 
                            target_tspi_status, command_machine_status, ground_angle_satisfaction_flag, zero_crossing_flag
                        ) VALUES (
                            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                        );
                        """,
                (PreparedStatement preparedStatement, AaTraj aaTraj) -> {
                    log.error("AaTraj : {}", aaTraj);
                    String sortieNumber = "20250303_五_01_ACT-3_邱陈_J16_07#02";
                    // 字符串转localDate sortieNumber.split("_")[0]
                    LocalDate localDate = LocalDate.parse(sortieNumber.split("_")[0], DateTimeFormatter.ofPattern("yyyyMMdd"));
                    // 注意 sortieNumber 是从配置里面来的 csv里面没有
                    preparedStatement.setString(1, sortieNumber);
                    preparedStatement.setString(2, aaTraj.getAircraftId());
                    // LocalTime解析不了 全部用Unix时间戳 Long型
                    preparedStatement.setLong(3, JavaTimeUtil.convertLocalTimeToUnixTimestamp(localDate, aaTraj.getMessageTime()));
                    preparedStatement.setLong(4, JavaTimeUtil.convertLocalTimeToUnixTimestamp(localDate, aaTraj.getSatelliteGuidanceTime()));
                    preparedStatement.setLong(5, JavaTimeUtil.convertLocalTimeToUnixTimestamp(localDate, aaTraj.getLocalTime()));
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
                },
                JdbcSinkUtil.getTiDBJdbcExecutionOptions(),
                JdbcSinkUtil.getTiDBJdbcConnectionOptions(TiDBDatabase.SIMULATION)
        );

        env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "file-source")
                .returns(AaTraj.class)
                .addSink(sinkFunction)
                .name("AaTraj File Sink");

        try {
            env.execute("AaTraj File Receiver");
        } catch (Exception e) {
            throw new ZorathosException(e, "Error occurs while executing the Flink job.");
        }
    }
}
