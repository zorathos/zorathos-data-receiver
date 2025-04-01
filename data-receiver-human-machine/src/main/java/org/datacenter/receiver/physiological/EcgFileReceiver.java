package org.datacenter.receiver.physiological;

import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.flink.util.function.SerializableFunction;
import org.datacenter.model.physiological.Ecg;
import org.datacenter.receiver.physiological.base.PhysiologicalFileReceiver;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author : [wangminan]
 * @description : 心电数据接收器 走csv解析
 */
public class EcgFileReceiver  extends PhysiologicalFileReceiver<Ecg> {

    @Override
    protected SerializableFunction<CsvMapper, CsvSchema> getSchemaGenerator() {
        return null;
    }

    @Override
    protected String getInsertQuery() {
        return "";
    }

    @Override
    protected void bindPreparedStatement(PreparedStatement preparedStatement, Ecg data, String sortieNumber) throws SQLException {

    }

    @Override
    protected JdbcStatementBuilder<Ecg> getJdbcStatementBuilder() {
        return null;
    }
}
