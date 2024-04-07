package com.alibaba.druid.bvt.sql.starrocks;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.visitor.SchemaStatVisitor;
import com.alibaba.druid.util.JdbcConstants;
import junit.framework.TestCase;

import java.util.List;
import java.util.stream.Collectors;

public class SQLLineageUtilsTest extends TestCase {
    public void test_getTables() {
        String sql = "with ps_manage_mode as (select name from dwd_ehr_user_category where category=1),\n" +
                "ps_user_type as (select name from dwd_ehr_user_category where category=2)\n" +
                "\n" +
                "SELECT t1.leave_total, t2.mtd, t3.ytd\n" +
                "    , t4.mtd_start_on, t5.mtd_end_on, 2*t2.mtd/(t4.mtd_start_on + t5.mtd_end_on)*100 AS mtd_percent\n" +
                "    , t6.ytd_percent_on, t3.ytd/t6.ytd_percent_on*100 AS ytd_percent\n" +
                "FROM (\n" +
                "    -- 离职总人数\n" +
                "    SELECT COUNT(*) AS leave_total\n" +
                "    FROM ods_ehr_user_leave_1d a\n" +
                "        LEFT JOIN ods_ehr_user_1d b ON a.user_id = b.user_id\n" +
                "        JOIN ods_ehr_department_1d c ON a.old_department_id = c.dept_id\n" +
                "    WHERE a.do_leave_day BETWEEN now() AND now()\n" +
                "        AND arrays_overlap(c.dept_id_arr, [1]) > 0\n" +
                "        AND arrays_overlap(c.dept_id_arr, [534]) > 0\n" +
                "        AND b.ps_manage_mode IN (select * from ps_manage_mode)\n" +
                "        AND b.ps_user_type IN (select * from ps_user_type)\n" +
                ") t1 JOIN (\n" +
                "    -- MTD总人数\n" +
                "    SELECT COUNT(*) AS mtd\n" +
                "    FROM ods_ehr_user_leave_1d a\n" +
                "        LEFT JOIN ods_ehr_user_1d b ON a.user_id = b.user_id\n" +
                "        JOIN ods_ehr_department_1d c ON a.old_department_id = c.dept_id\n" +
                "    WHERE a.do_leave_day BETWEEN DATE_TRUNC(\"month\", now()) AND now()\n" +
                "        AND arrays_overlap(c.dept_id_arr, [1]) > 0\n" +
                "        AND arrays_overlap(c.dept_id_arr, [534]) > 0\n" +
                "        AND b.ps_manage_mode IN (select * from ps_manage_mode)\n" +
                "        AND b.ps_user_type IN (select * from ps_user_type)\n" +
                ") t2 JOIN (\n" +
                "    -- YTD总人数\n" +
                "    SELECT COUNT(*) AS ytd\n" +
                "    FROM ods_ehr_user_leave_1d a\n" +
                "        LEFT JOIN ods_ehr_user_1d b ON a.user_id = b.user_id\n" +
                "        JOIN ods_ehr_department_1d c ON a.old_department_id = c.dept_id\n" +
                "    WHERE a.do_leave_day BETWEEN DATE_TRUNC(\"year\", now()) AND now()\n" +
                "        AND arrays_overlap(c.dept_id_arr, [1]) > 0\n" +
                "        AND arrays_overlap(c.dept_id_arr, [534]) > 0\n" +
                "        AND b.ps_manage_mode IN (select * from ps_manage_mode)\n" +
                "        AND b.ps_user_type IN (select * from ps_user_type)\n" +
                ") t3 JOIN (\n" +
                "    -- MTD期初在职人数\n" +
                "    SELECT COUNT(*) AS mtd_start_on\n" +
                "    FROM dwd_ehr_user_df a\n" +
                "        JOIN ods_ehr_user_1d k ON a.user_id=k.user_id\n" +
                "        JOIN ods_ehr_department_1d b ON k.dept_id = b.dept_id\n" +
                "    WHERE a.date=to_date(DATE_SUB(now(), INTERVAL DAY(now()) DAY))\n" +
                "        AND a.is_active=1\n" +
                "        AND arrays_overlap(b.dept_id_arr, [1]) > 0\n" +
                "        AND arrays_overlap(b.dept_id_arr, [534]) > 0\n" +
                "        AND k.ps_manage_mode IN (select * from ps_manage_mode)\n" +
                "        AND k.ps_user_type IN (select * from ps_user_type)\n" +
                ") t4 JOIN (\n" +
                "    -- MTD期末在职人数\n" +
                "    SELECT COUNT(*) AS mtd_end_on\n" +
                "    FROM dwd_ehr_user_df a\n" +
                "        JOIN ods_ehr_user_1d k ON a.user_id=k.user_id\n" +
                "        JOIN ods_ehr_department_1d b ON k.dept_id = b.dept_id\n" +
                "    WHERE a.date=to_date(DATE_SUB(curdate(), INTERVAL 1 DAY))\n" +
                "        AND a.is_active=1\n" +
                "        AND arrays_overlap(b.dept_id_arr, [1]) > 0\n" +
                "        AND arrays_overlap(b.dept_id_arr, [534]) > 0\n" +
                "        AND k.ps_manage_mode IN (select * from ps_manage_mode)\n" +
                "        AND k.ps_user_type IN (select * from ps_user_type)\n" +
                ") t5 JOIN (\n" +
                "    -- YTD月末在职人均数\n" +
                "    SELECT IF(COUNT (*)>0, SUM (nums) / COUNT (*), 0) AS ytd_percent_on\n" +
                "    FROM (\n" +
                "        SELECT join_dt, COUNT (*) AS nums\n" +
                "        FROM (\n" +
                "            SELECT t.unnest AS join_dt\n" +
                "            FROM (\n" +
                "                SELECT [to_date(DATE_SUB(now(), INTERVAL 1 DAY)),\n" +
                "                    to_date(DATE_SUB(DATE_SUB(now(), INTERVAL DAY (now()) DAY), INTERVAL 0 MONTH)),\n" +
                "                    to_date(DATE_SUB(DATE_SUB(now(), INTERVAL DAY (now()) DAY), INTERVAL 1 MONTH)),\n" +
                "                    to_date(DATE_SUB(DATE_SUB(now(), INTERVAL DAY (now()) DAY), INTERVAL 2 MONTH)),\n" +
                "                    to_date(DATE_SUB(DATE_SUB(now(), INTERVAL DAY (now()) DAY), INTERVAL 3 MONTH)),\n" +
                "                    to_date(DATE_SUB(DATE_SUB(now(), INTERVAL DAY (now()) DAY), INTERVAL 4 MONTH)),\n" +
                "                    to_date(DATE_SUB(DATE_SUB(now(), INTERVAL DAY (now()) DAY), INTERVAL 5 MONTH)),\n" +
                "                    to_date(DATE_SUB(DATE_SUB(now(), INTERVAL DAY (now()) DAY), INTERVAL 6 MONTH)),\n" +
                "                    to_date(DATE_SUB(DATE_SUB(now(), INTERVAL DAY (now()) DAY), INTERVAL 7 MONTH)),\n" +
                "                    to_date(DATE_SUB(DATE_SUB(now(), INTERVAL DAY (now()) DAY), INTERVAL 8 MONTH)),\n" +
                "                    to_date(DATE_SUB(DATE_SUB(now(), INTERVAL DAY (now()) DAY), INTERVAL 9 MONTH)),\n" +
                "                    to_date(DATE_SUB(DATE_SUB(now(), INTERVAL DAY (now()) DAY), INTERVAL 10 MONTH))\n" +
                "                ] as need_join_dts\n" +
                "            ) as tmp_dts CROSS JOIN LATERAL UNNEST(need_join_dts) AS t\n" +
                "            WHERE YEAR (t.unnest)= YEAR (now())\n" +
                "        ) tmp_join_dts\n" +
                "            JOIN dwd_ehr_user_df a ON join_dt=a.date\n" +
                "            JOIN ods_ehr_user_1d k ON a.user_id=k.user_id\n" +
                "            JOIN ods_ehr_department_1d b ON k.dept_id=b.dept_id\n" +
                "        WHERE a.is_active=1 AND arrays_overlap(b.dept_id_arr, [1]) > 0\n" +
                "            AND arrays_overlap(b.dept_id_arr, [534]) > 0\n" +
                "            AND k.ps_manage_mode IN (select * from ps_manage_mode)\n" +
                "            AND k.ps_user_type IN (select * from ps_user_type)\n" +
                "        GROUP BY join_dt\n" +
                "    ) tmp\n" +
                ") t6";

        final DbType dbType = JdbcConstants.STARROCKS;
        List<SQLStatement> sqlStatements = SQLUtils.parseStatements(sql, dbType);
        SQLStatement stmt = sqlStatements.get(0);

        SchemaStatVisitor statVisitor = SQLUtils.createSchemaStatVisitor(dbType);
        stmt.accept(statVisitor);
        List<String> tables = statVisitor.getTables().keySet().stream().map(x -> x.getName()).collect(Collectors.toList());
        System.out.println(tables);

    }


    public void test_getTables2() {
        String sql =
                        "            SELECT t.unnest AS join_dt\n" +
                        "            FROM  tmp_dts CROSS JOIN LATERAL UNNEST(need_join_dts) AS t\n" +
                        "            WHERE YEAR (t.unnest)= YEAR (now())\n";

        final DbType dbType = JdbcConstants.STARROCKS;
        List<SQLStatement> sqlStatements = SQLUtils.parseStatements(sql, dbType);
        SQLStatement stmt = sqlStatements.get(0);

        SchemaStatVisitor statVisitor = SQLUtils.createSchemaStatVisitor(dbType);
        stmt.accept(statVisitor);
        List<String> tables = statVisitor.getTables().keySet().stream().map(x -> x.getName()).collect(Collectors.toList());
        System.out.println(tables);

    }
}
