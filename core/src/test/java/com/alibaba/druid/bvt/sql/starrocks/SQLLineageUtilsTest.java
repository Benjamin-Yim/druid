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

    public void test_getTables1() {
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


    public void test_getTables2() {
        String sql =
                "            SELECT t.unnest AS join_dt\n" +
                        "            FROM  tmp_dts CROSS JOIN LATERAL UNNEST(need_join_dts) AS t\n" +
                        "            WHERE YEAR (t.unnest)= YEAR (now())\n" +
                        "            UNION ALL\n" +
                        "            SELECT t.unnest AS join_dt\n" +
                        "            FROM  tmp_dts2 CROSS JOIN LATERAL UNNEST(need_join_dts) AS t\n" +
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
