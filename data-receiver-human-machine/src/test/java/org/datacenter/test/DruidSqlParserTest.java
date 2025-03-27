package org.datacenter.test;

import com.alibaba.druid.sql.ast.statement.SQLCreateTableStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import lombok.extern.slf4j.Slf4j;

/**
 * @author : [wangminan]
 * @description : [一句话描述该类的功能]
 */
@Slf4j
public class DruidSqlParserTest {

    public static void main(String[] args) {
        String sql = """
                CREATE TABLE ACMI_002_tspi_0305 (
                    auto_id BIGINT,
                    batch_id VARCHAR(255),
                    sortie_id VARCHAR(255),
                    code1 BIGINT,
                    code2 BIGINT,
                    code3 VARCHAR(255),
                    code4 VARCHAR(255),
                    code5 VARCHAR(255),
                    code6 VARCHAR(255)
                )
                DUPLICATE KEY(auto_id, batch_id, sortie_id, code1)
                DISTRIBUTED BY HASH(auto_id) BUCKETS 10
                PROPERTIES (
                    "replication_num" = "1"
                );
                """;

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLCreateTableStatement sqlCreateTableStatement = parser.parseCreateTable();
        log.info("ddl:{}", sqlCreateTableStatement);
    }
}
