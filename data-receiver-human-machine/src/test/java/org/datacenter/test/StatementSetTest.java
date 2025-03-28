package org.datacenter.test;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author : [wangminan]
 * @description : [一句话描述该类的功能]
 */
public class StatementSetTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        StatementSet statementSet = tableEnv.createStatementSet();


        // 动态创建表并添加到StatementSet
        for (int i = 0; i < 2; i++) {
            String sourceTable = "source_" + i;
            String targetTable = "target_" + i;

            // 创建源表
            tableEnv.executeSql(
                    "CREATE TABLE " + sourceTable + " (...) WITH ('connector' = 'jdbc', ...)"
            );

            // 创建目标表
            tableEnv.executeSql(
                    "CREATE TABLE " + targetTable + " (...) WITH ('connector' = 'jdbc', ...)"
            );

            // 添加到StatementSet
            Table table = tableEnv.from(sourceTable);
            statementSet.addInsert(targetTable, table);
        }

        // 一次性执行所有语句
        statementSet.execute();
    }
}
