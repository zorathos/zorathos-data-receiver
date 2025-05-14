package org.datacenter.receiver.physiological.base;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.datacenter.config.receiver.physiological.PhysiologicalFileReceiverConfig;
import org.datacenter.receiver.CsvFileReceiver;

import java.io.Serial;
import java.io.Serializable;

/**
 * @author : [wangminan]
 * @description : 抽象的生理数据离线接入器
 */
@Slf4j
@Data
@EqualsAndHashCode(callSuper = true)
public abstract class PhysiologicalFileReceiver<T> extends CsvFileReceiver<T, PhysiologicalFileReceiverConfig> implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    @Override
    public JdbcStatementBuilder<T> getJdbcStatementBuilder() {
        Long importId = config.getImportId();
        return ((preparedStatement, data) -> bindPreparedStatement(preparedStatement, data, importId));
    }
}
