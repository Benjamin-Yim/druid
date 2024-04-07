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
                "PARTITION BY RANGE(`event_day`)\n" +
                "(PARTITION p20231017 VALUES [(\"2023-10-17\"), (\"2023-10-18\")),\n" +
                "PARTITION p20231018 VALUES [(\"2023-10-18\"), (\"2023-10-19\")),\n" +
                "PARTITION p20231019 VALUES [(\"2023-10-19\"), (\"2023-10-20\")),\n" +
                "PARTITION p20231020 VALUES [(\"2023-10-20\"), (\"2023-10-21\")),\n" +
                "PARTITION p20231021 VALUES [(\"2023-10-21\"), (\"2023-10-22\")))\n" +
                "DISTRIBUTED BY HASH(`event_day`) BUCKETS 16 \n" +
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
        String sql = "create table fsadasdf (\n" +
                "  `d` DATE  COMMENT \"dkkdkdk\",\n" +
                "  `ee` varchar(64) default null comment \"eeeee\",\n" +
                "  `dddd` varchar(64) default null comment \"fffff\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`event_day`, `event_id`)\n" +
                "PARTITION BY RANGE (event_day) (\n" +
                "    START (\"2023-10-17\") END (\"2023-10-22\") EVERY (INTERVAL 1 DAY)\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(`event_day`) BUCKETS 16\n" +
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
        String sql = "create table fsadasdf (\n" +
                "  `d` DATE  COMMENT \"dkkdkdk\",\n" +
                "  `ee` varchar(64) default null comment \"eeeee\",\n" +
                "  `dddd` varchar(64) default null comment \"fffff\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`event_day`, `event_id`)\n" +
                "PARTITION BY RANGE (event_day) (\n" +
                "    START (\"2023-10-17\") END (\"2023-10-22\") EVERY (INTERVAL)\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(`event_day`) BUCKETS 16\n" +
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
        String sql = "CREATE TABLE `aggregate_tbl` (\n" +
                "  `site_id` largeint(40) NOT NULL COMMENT \"id of site\",\n" +
                "  `date` date NOT NULL COMMENT \"time of event\",\n" +
                "  `city_code` varchar(20) NULL COMMENT \"city_code of user\",\n" +
                "  `pv` bigint(20) SUM NULL DEFAULT \"0\" COMMENT \"total page views\",\n" +
                "  `percent` percentile PERCENTILE_UNION NULL COMMENT \"others\"\n" +
                ") ENGINE=OLAP \n" +
                "AGGREGATE KEY(`site_id`, `date`, `city_code`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`site_id`) BUCKETS 8 \n" +
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
        String sql = "CREATE TABLE `commodity_brand` (\n" +
                "  INDEX idx_name (`name`) USING BITMAP COMMENT ''\n" +
                ") ENGINE=OLAP \n" +
                "PRIMARY KEY(`id`)\n" +
                "COMMENT \"品牌表\"\n" +
                "DISTRIBUTED BY HASH(`id`) BUCKETS 8 \n" +
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
        String sql = "CREATE TABLE `dtsy_relation` (\n" +
                "  `id` bigint(20) NOT NULL COMMENT \"自增id\",\n" +
                "  `updated_by` varchar(65533) NULL COMMENT \"更新人\"\n" +
                ") ENGINE=OLAP \n" +
                "PRIMARY KEY(`id`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`id`) BUCKETS 8 \n" +
                "ORDER BY(`datasource_id`, `datasource_user_id`)\n" +
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
        String sql = "CREATE TABLE `page_resource_log_agg` (\n" +
                "  `puid` bitmap BITMAP_UNION NOT NULL COMMENT \"puid聚合\"\n" +
                ") ENGINE=OLAP \n" +
                "AGGREGATE KEY(`date`, `system_id`, `page`, `resource_type`, `resource`, `is_first`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE(`date`)\n" +
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
