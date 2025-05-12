package org.datacenter.receiver.crew;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.datacenter.config.receiver.crew.PersonnelJsonFileReceiverConfig;
import org.datacenter.model.crew.PersonnelInfo;
import org.datacenter.receiver.BaseReceiver;
import org.datacenter.receiver.crew.util.PersonnelSinkUtil;
import org.datacenter.receiver.util.DataReceiverUtil;
import org.datacenter.receiver.util.JsonArrayFileInputFormat;

import static org.datacenter.config.keys.HumanMachineReceiverConfigKey.IMPORT_ID;
import static org.datacenter.config.keys.HumanMachineReceiverConfigKey.PERSONNEL_URL;

/**
 * @author : [wangminan]
 * @description : [人员信息JSON接收器]
 */
@Slf4j
public class PersonnelJsonFileReceiver extends BaseReceiver {

    private final PersonnelJsonFileReceiverConfig receiverConfig;

    public PersonnelJsonFileReceiver(PersonnelJsonFileReceiverConfig receiverConfig) {
        this.receiverConfig = receiverConfig;
    }

    @Override
    public void prepare() {
        super.prepare();
    }

    @Override
    public void start() {
        StreamExecutionEnvironment env = DataReceiverUtil.prepareStreamEnv();

        JsonArrayFileInputFormat<PersonnelInfo> inputFormat = new JsonArrayFileInputFormat<>(PersonnelInfo.class);

        FileSource<PersonnelInfo> source = FileSource
                .forRecordStreamFormat(inputFormat, new Path(receiverConfig.getUrl()))
                .build();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "PersonnelInfo")
                .returns(PersonnelInfo.class)
                .sinkTo(PersonnelSinkUtil.getPilotJdbcSink(receiverConfig.getImportId()));

        try {
            env.execute("PersonnelJsonFileReceiver");
        } catch (Exception e) {
            log.error("Error executing PersonnelJsonFileReceiver", e);
        }
    }

    public static void main(String[] args) {
        ParameterTool params = ParameterTool.fromArgs(args);
        log.info("Parameters: {}", params.toMap());
        PersonnelJsonFileReceiverConfig receiverConfig = PersonnelJsonFileReceiverConfig.builder()
                .url(params.getRequired(PERSONNEL_URL.getKeyForParamsMap()))
                .importId(params.getRequired(IMPORT_ID.getKeyForParamsMap()))
                .build();

        PersonnelJsonFileReceiver receiver = new PersonnelJsonFileReceiver(receiverConfig);
        receiver.run();
    }
}
