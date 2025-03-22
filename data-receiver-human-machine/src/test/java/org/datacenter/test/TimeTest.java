package org.datacenter.test;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
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
import org.codehaus.janino.Java;
import org.datacenter.config.system.HumanMachineSysConfig;
import org.datacenter.exception.ZorathosException;
import org.datacenter.model.base.TiDBDatabase;
import org.datacenter.receiver.util.DataReceiverUtil;
import org.datacenter.receiver.util.JavaTimeUtil;
import org.datacenter.receiver.util.JdbcSinkUtil;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

@Slf4j
public class TimeTest {

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
        HumanMachineSysConfig humanMachineSysConfig = new HumanMachineSysConfig();
        humanMachineSysConfig.loadConfig();

        StreamExecutionEnvironment env = DataReceiverUtil.prepareStreamEnv();

        // 列解析需要手动指定列顺序
        SerializableFunction<CsvMapper, CsvSchema> schemaGenerator = mapper -> CsvSchema.builder()
                .addColumn("messageTime")
                .addColumn("satelliteGuidanceTime")
                .addColumn("localTime")
                .setUseHeader(true)
                .setColumnSeparator(',')
                .setLineSeparator("\n")
                .build();

        CsvReaderFormat<SimpleSortie> csvReaderFormat = CsvReaderFormat.forSchema(
                (SerializableSupplier<CsvMapper>) () -> {
                    CsvMapper csvMapper = new CsvMapper();
                    csvMapper.registerModule(new JavaTimeModule());
                    return csvMapper;
                },
                schemaGenerator,
                TypeInformation.of(SimpleSortie.class)
        );

        // 定义 FileSource
        FileSource<SimpleSortie> fileSource = FileSource.forRecordStreamFormat(
                csvReaderFormat,
                new Path("D:\\test.csv")
        ).build();

        // 定义 SinkFunction
        SinkFunction<SimpleSortie> sinkFunction = JdbcSink.sink(
                """
                        INSERT INTO test (sortie_number, message_time, satellite_guidance_time, local_time)
                        VALUES (?, ?, ?, ?)
                        """,
                (PreparedStatement preparedStatement, SimpleSortie simpleSortie) -> {
                    log.error("simpleSortie: {}", simpleSortie);
                    LocalDate localDate = LocalDate.parse("20250303_五_01_ACT-3_邱陈_J16_07#02".split("_")[0], DateTimeFormatter.ofPattern("yyyyMMdd"));
                    preparedStatement.setString(1, "20250303_五_01_ACT-3_邱陈_J16_07#02");
                    // 将 LocalTime 转成 Unix 时间戳（毫秒）
                    preparedStatement.setLong(2, JavaTimeUtil.convertLocalTimeToUnixTimestamp(localDate, simpleSortie.getMessageTime()));
                    preparedStatement.setLong(3, JavaTimeUtil.convertLocalTimeToUnixTimestamp(localDate, simpleSortie.getSatelliteGuidanceTime()));
                    preparedStatement.setLong(4, JavaTimeUtil.convertLocalTimeToUnixTimestamp(localDate, simpleSortie.getLocalTime()));
                },
                JdbcSinkUtil.getTiDBJdbcExecutionOptions(),
                JdbcSinkUtil.getTiDBJdbcConnectionOptions(TiDBDatabase.SIMULATION)
        );

        env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "file-source")
                .returns(SimpleSortie.class)
                // 将 LocalTime 转成时间戳
//                .map(sortie -> {
//                    sortie.setLocalTime(sortie.getLocalTime());
//                    return sortie;
//                })
//                .print()
                .addSink(sinkFunction)
                .name("SimpleSortie File Sink");

        try {
            env.execute("SimpleSortie File Receiver");
        } catch (Exception e) {
            throw new ZorathosException(e, "Error occurs while executing the Flink job.");
        }
    }
}
