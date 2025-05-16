package org.datacenter.receiver.environment.base;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.datacenter.config.receiver.environment.EnvironmentFileReceiverConfig;
import org.datacenter.receiver.CsvFileReceiver;

import java.io.Serial;
import java.io.Serializable;

import static org.datacenter.config.keys.HumanMachineReceiverConfigKey.ENVIRONMENT_FILE_URL;
import static org.datacenter.config.keys.HumanMachineReceiverConfigKey.IMPORT_ID;

/**
 * @author : [wangminan]
 * @description : 抽象的环境文件接收器
 */
@Slf4j
@Data
@EqualsAndHashCode(callSuper = true)
public abstract class EnvironmentFileReceiver<T> extends CsvFileReceiver<T, EnvironmentFileReceiverConfig> implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    @Override
    public JdbcStatementBuilder<T> getJdbcStatementBuilder() {
        Long importId = config.getImportId();
        return ((preparedStatement, data) -> bindPreparedStatement(preparedStatement, data, importId));
    }

    protected static EnvironmentFileReceiverConfig parseArgs(String[] args) {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        return EnvironmentFileReceiverConfig.builder()
                .importId(Long.valueOf(parameterTool.getRequired(IMPORT_ID.getKeyForParamsMap())))
                .url(parameterTool.getRequired(ENVIRONMENT_FILE_URL.getKeyForParamsMap()))
                .build();
    }
}
