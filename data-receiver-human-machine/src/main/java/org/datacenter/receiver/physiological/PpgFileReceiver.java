package org.datacenter.receiver.physiological;

import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.flink.util.function.SerializableFunction;
import org.datacenter.model.physiological.Ppg;
import org.datacenter.receiver.physiological.base.PhysiologicalFileReceiver;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author : [wangminan]
 * @description : Ppg数据接收器 走csv解析
 */
public class PpgFileReceiver extends PhysiologicalFileReceiver<Ppg> {

    @Override
    protected SerializableFunction<CsvMapper, CsvSchema> getSchemaGenerator() {
        return null;
    }

    @Override
    protected String getInsertQuery() {
        return "";
    }

    @Override
    protected void bindPreparedStatement(PreparedStatement preparedStatement, Ppg data, String sortieNumber) throws SQLException {

    }

    @Override
    protected JdbcStatementBuilder<Ppg> getJdbcStatementBuilder() {
        return null;
    }
}
