package com.alibaba.druid.bvt.sql.starrocks;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.starrocks.ast.StarrocksCreateTableStatement;
import com.alibaba.druid.util.JdbcConstants;
import junit.framework.TestCase;

import java.util.List;
import java.util.Map;

public class StarrocksCreateTest1 extends TestCase {

    public void test_0() throws Exception {
        String sql = "CREATE TABLE `dfdsfadsfsd` (\n" +
                "  `a` date NULL COMMENT \"a\",\n" +
                "  `b` varchar(64) NULL COMMENT \"b\",\n" +
                "  `c` varchar(64) NULL COMMENT \"c\"\n" +
                ") ENGINE=OLAP \n" +
                "DUPLICATE KEY(`a`, `b`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE(`a`)\n" +
                "(PARTITION p20231017 VALUES [(\"2023-10-17\"), (\"2023-10-18\")),\n" +
                "PARTITION p20231018 VALUES [(\"2023-10-18\"), (\"2023-10-19\")),\n" +
                "PARTITION p20231019 VALUES [(\"2023-10-19\"), (\"2023-10-20\")),\n" +
                "PARTITION p20231020 VALUES [(\"2023-10-20\"), (\"2023-10-21\")),\n" +
                "PARTITION p20231021 VALUES [(\"2023-10-21\"), (\"2023-10-22\")))\n" +
                "DISTRIBUTED BY HASH(`a`) BUCKETS 16 \n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"3\",\n" +
                "\"dynamic_partition.enable\" = \"true\",\n" +
                "\"dynamic_partition.time_unit\" = \"DAY\",\n" +
                "\"dynamic_partition.time_zone\" = \"Asia/Shanghai\",\n" +
                "\"dynamic_partition.start\" = \"234124\",\n" +
                "\"dynamic_partition.end\" = \"5\",\n" +
                "\"dynamic_partition.prefix\" = \"p\",\n" +
                "\"dynamic_partition.buckets\" = \"16\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\",\n" +
                "\"enable_persistent_index\" = \"false\",\n" +
                "\"compression\" = \"LZ4\"\n" +
                ");";
        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, JdbcConstants.STARROCKS);
        StarrocksCreateTableStatement stmt = (StarrocksCreateTableStatement) stmtList.get(0);
        for (Map.Entry<String, String> item : stmt.properties.entrySet()) {
            System.out.println(item.getKey() + ":" + item.getValue());
        }
    }
    public void test_1() throws Exception {
        String sql = "create table table1 (\n" +
                "  `b` DATE  COMMENT \"b\",\n" +
                "  `c` varchar(64) default null comment \"c\",\n" +
                "  `d` varchar(64) default null comment \"d\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`b`, `c`)\n" +
                "PARTITION BY RANGE (b) (\n" +
                "    START (\"2023-10-17\") END (\"2023-10-22\") EVERY (INTERVAL 1 DAY)\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(`b`) BUCKETS 16\n" +
                "PROPERTIES (\n" +
                "\"dynamic_partition.enable\" = \"true\",\n" +
                "\"dynamic_partition.time_unit\" = \"DAY\",\n" +
                "\"dynamic_partition.end\" = \"5\",\n" +
                "\"dynamic_partition.prefix\" = \"p\",\n" +
                "\"dynamic_partition.buckets\" = \"16\",\n" +
                "\"replication_num\" = \"3\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\",\n" +
                "\"enable_persistent_index\" = \"false\",\n" +
                "\"compression\" = \"LZ4\"\n" +
                ");";
        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, JdbcConstants.STARROCKS);
        StarrocksCreateTableStatement stmt = (StarrocksCreateTableStatement) stmtList.get(0);
        for (Map.Entry<String, String> item : stmt.properties.entrySet()) {
            System.out.println(item.getKey() + ":" + item.getValue());
        }
    }

    public void test_2() throws Exception {
        String sql = "create table a (\n" +
                "  `b` DATE  COMMENT \"b\",\n" +
                "  `c` varchar(64) default null comment \"c\",\n" +
                "  `d` varchar(64) default null comment \"d\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`b`, `c`)\n" +
                "PARTITION BY RANGE (b) (\n" +
                "    START (\"2023-10-17\") END (\"2023-10-22\") EVERY (INTERVAL)\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(`b`) BUCKETS 16\n" +
                "PROPERTIES (\n" +
                "\"dynamic_partition.enable\" = \"true\",\n" +
                "\"dynamic_partition.time_unit\" = \"DAY\",\n" +
                "\"dynamic_partition.end\" = \"5\",\n" +
                "\"dynamic_partition.prefix\" = \"p\",\n" +
                "\"dynamic_partition.buckets\" = \"16\",\n" +
                "\"replication_num\" = \"3\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\",\n" +
                "\"enable_persistent_index\" = \"false\",\n" +
                "\"compression\" = \"LZ4\"\n" +
                ");";
        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, JdbcConstants.STARROCKS);
        StarrocksCreateTableStatement stmt = (StarrocksCreateTableStatement) stmtList.get(0);
        for (Map.Entry<String, String> item : stmt.properties.entrySet()) {
            System.out.println(item.getKey() + ":" + item.getValue());
        }
    }


    public void test_3() throws Exception {
        String sql = "CREATE TABLE `a` (\n" +
                "  `f` percentile PERCENTILE_UNION NULL COMMENT \"f\",\n" +
                "  `b` largeint(40) NOT NULL COMMENT \"b\",\n" +
                "  `c` date NOT NULL COMMENT \"c\",\n" +
                "  `d` varchar(20) NULL COMMENT \"d\",\n" +
                "  `e` bigint(20) SUM NULL DEFAULT \"0\" COMMENT \"e\"\n" +
                ") ENGINE=OLAP \n" +
                "AGGREGATE KEY(`b`, `c`, `d`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`b`) BUCKETS 8 \n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\",\n" +
                "\"enable_persistent_index\" = \"false\",\n" +
                "\"compression\" = \"LZ4\"\n" +
                ");";
        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, JdbcConstants.STARROCKS);
        StarrocksCreateTableStatement stmt = (StarrocksCreateTableStatement) stmtList.get(0);
        for (Map.Entry<String, String> item : stmt.properties.entrySet()) {
            System.out.println(item.getKey() + ":" + item.getValue());
        }
    }

    public void test_4() throws Exception {
        String sql = "CREATE TABLE `a` (\n" +
                "  INDEX idx_name (`b`) USING BITMAP COMMENT ''\n" +
                ") ENGINE=OLAP \n" +
                "PRIMARY KEY(`b`)\n" +
                "COMMENT \"c\"\n" +
                "DISTRIBUTED BY HASH(`b`) BUCKETS 8 \n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"3\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\",\n" +
                "\"enable_persistent_index\" = \"false\",\n" +
                "\"compression\" = \"LZ4\"\n" +
                ");";
        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, JdbcConstants.STARROCKS);
        StarrocksCreateTableStatement stmt = (StarrocksCreateTableStatement) stmtList.get(0);
        for (Map.Entry<String, String> item : stmt.properties.entrySet()) {
            System.out.println(item.getKey() + ":" + item.getValue());
        }
    }

    public void test_5() throws Exception {
        String sql = "CREATE TABLE `a` (\n" +
                "  `b` bigint(20) NOT NULL COMMENT \"b\",\n" +
                "  `c` varchar(65533) NULL COMMENT \"c\"\n" +
                ") ENGINE=OLAP \n" +
                "PRIMARY KEY(`b`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`b`) BUCKETS 8 \n" +
                "ORDER BY(`b`, `c`)\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"3\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\",\n" +
                "\"enable_persistent_index\" = \"true\",\n" +
                "\"compression\" = \"LZ4\"\n" +
                ");";
        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, JdbcConstants.STARROCKS);
        StarrocksCreateTableStatement stmt = (StarrocksCreateTableStatement) stmtList.get(0);
        for (Map.Entry<String, String> item : stmt.properties.entrySet()) {
            System.out.println(item.getKey() + ":" + item.getValue());
        }
    }


    public void test_6() throws Exception {
        String sql = "CREATE TABLE `a` (\n" +
                "  `b` bitmap BITMAP_UNION NOT NULL COMMENT \"b\"\n" +
                ") ENGINE=OLAP \n" +
                "AGGREGATE KEY(`b`, `c`, `d`, `e`, `f`, `g`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE(`b`)\n" +
                "(PARTITION p20230402 VALUES [(\"2023-04-02\"), (\"2023-04-03\")),\n" +
                "PARTITION p20240407 VALUES [(\"2024-04-07\"), (\"2024-04-08\")))\n" +
                "DISTRIBUTED BY HASH(`date`, `system_id`, `page`) BUCKETS 16 \n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"3\",\n" +
                "\"dynamic_partition.enable\" = \"true\",\n" +
                "\"dynamic_partition.time_unit\" = \"DAY\",\n" +
                "\"dynamic_partition.time_zone\" = \"Asia/Shanghai\",\n" +
                "\"dynamic_partition.start\" = \"-366\",\n" +
                "\"dynamic_partition.end\" = \"5\",\n" +
                "\"dynamic_partition.prefix\" = \"p\",\n" +
                "\"dynamic_partition.buckets\" = \"16\",\n" +
                "\"dynamic_partition.history_partition_num\" = \"0\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\",\n" +
                "\"enable_persistent_index\" = \"false\",\n" +
                "\"compression\" = \"LZ4\"\n" +
                ");";
        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, JdbcConstants.STARROCKS);
        StarrocksCreateTableStatement stmt = (StarrocksCreateTableStatement) stmtList.get(0);
        for (Map.Entry<String, String> item : stmt.properties.entrySet()) {
            System.out.println(item.getKey() + ":" + item.getValue());
        }
    }

}
