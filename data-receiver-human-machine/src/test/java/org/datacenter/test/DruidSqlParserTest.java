package org.datacenter.test;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.druid.sql.ast.statement.SQLSelectOrderByItem;
import com.alibaba.druid.sql.dialect.doris.parser.DorisStatementParser;
import com.alibaba.druid.sql.dialect.starrocks.ast.statement.StarRocksCreateTableStatement;
import com.alibaba.druid.util.JdbcConstants;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author : [wangminan]
 * @description : [一句话描述该类的功能]
 */
@Slf4j
public class DruidSqlParserTest {

    public static void main(String[] args) {
        String tableName = "ACMI_002_tspi_0305";
        String sql = """
                CREATE TABLE %s (
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
                """.formatted(tableName);

        DorisStatementParser dorisStatementParser = new DorisStatementParser(sql);
        // Doris 的 parser 实现了 StarRocksCreateTableParser
        StarRocksCreateTableStatement createTableStatement = (StarRocksCreateTableStatement) dorisStatementParser.getSQLCreateTableParser().parseCreateTable();
        log.info("ddl:{}", createTableStatement);
        List<SQLSelectOrderByItem> columns = createTableStatement.getUnique().getIndexDefinition().getColumns();

        createTableStatement.getTableElementList().stream().map(
                tableElement -> {
                    return tableElement.toString();
                });
        // 我只要表名和字段信息 给TiDB用 剩下的东西不要
        String tidbSql = """
                CREATE TABLE IF NOT EXISTS %s (
                    auto_id BIGINT PRIMARY KEY,
                    batch_id VARCHAR(255),
                    sortie_id VARCHAR(255),
                    %s,
                    PRIMARY KEY (auto_id, batch_id, sortie_id, code1)
                );
                """.formatted(
                tableName,
                createTableStatement.getTableElementList().stream().map(
                                tableElement -> {
                                    SQLColumnDefinition columnDefinition = (SQLColumnDefinition) tableElement;
                                    String columnName = columnDefinition.getName().getSimpleName();
                                    if (!columnName.equals("auto_id") && !columnName.equals("batch_id") && !columnName.equals("sortie_id")) {
                                        return tableElement.toString();
                                    }
                                    return null;
                                }
                        ).filter(Objects::nonNull)
                        .collect(Collectors.joining(",\n"))
        );

        String format = SQLUtils.format(tidbSql, JdbcConstants.TIDB, SQLUtils.DEFAULT_LCASE_FORMAT_OPTION);

        log.info("tidbSql:{}", format);

        // 我只要表名和字段信息 给TiDB用 剩下的东西不要
        String tidb2 = """
                create table if not exists ACMI_002_tspi_0305 (
                 	auto_id BIGINT,
                 	batch_id VARCHAR(255),
                 	sortie_id VARCHAR(255),
                 	code1 BIGINT,
                 	code2 BIGINT,
                 	code3 VARCHAR(255),
                 	code4 VARCHAR(255),
                 	code5 VARCHAR(255),
                 	code6 VARCHAR(255),
                    PRIMARY KEY (auto_id, batch_id, sortie_id, code1)
                 );
                """;
        // 正则表达式 把 VARCHAR(*) 替换成 STRING
        String tidb3 = tidb2.replaceAll("VARCHAR\\(\\d+\\)", "STRING")
                // primary key (*) 换成 primary key (*) not enforced
                .replaceAll("PRIMARY KEY \\((.*?)\\)", "PRIMARY KEY ($1) NOT ENFORCED");
        log.info("tidbSql:{}", tidb3);
    }
}
