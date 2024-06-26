/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.druid.bvt.sql.starrocks;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.visitor.SchemaStatVisitor;
import com.alibaba.druid.util.JdbcConstants;
import junit.framework.TestCase;

import java.util.List;

public class StarrocksSelectTest34 extends TestCase {
    public void test_select() throws Exception {
        // 1095288847322
        String sql = "select a.city, a.car_id, a.rrc_id, a.brand, a.car_series, a.title\n" +
                "from dw_x001_cp_used_car_detail a\n" +
                "where status = 'PUBLISHED'\n" +
                "and model_id = 4419\n" +
                "and abs(datediff(licensed_date, '2013-07-01 00:00:00', 'mm')) <= 5\n" +
                "and abs(mileage - 7.01) < 3;";//
        assertEquals("SELECT a.city, a.car_id, a.rrc_id, a.brand, a.car_series\n" +
                "\t, a.title\n" +
                "FROM dw_x001_cp_used_car_detail a\n" +
                "WHERE status = 'PUBLISHED'\n" +
                "\tAND model_id = 4419\n" +
                "\tAND abs(datediff(licensed_date, '2013-07-01 00:00:00', 'mm')) <= 5\n" +
                "\tAND abs(mileage - 7.01) < 3;", SQLUtils.formatStarrocks(sql));

        assertEquals("select a.city, a.car_id, a.rrc_id, a.brand, a.car_series\n" +
                "\t, a.title\n" +
                "from dw_x001_cp_used_car_detail a\n" +
                "where status = 'PUBLISHED'\n" +
                "\tand model_id = 4419\n" +
                "\tand abs(datediff(licensed_date, '2013-07-01 00:00:00', 'mm')) <= 5\n" +
                "\tand abs(mileage - 7.01) < 3;", SQLUtils.formatStarrocks(sql, SQLUtils.DEFAULT_LCASE_FORMAT_OPTION));

        List<SQLStatement> statementList = SQLUtils.parseStatements(sql, JdbcConstants.STARROCKS);
        SQLStatement stmt = statementList.get(0);

        assertEquals(1, statementList.size());

        SchemaStatVisitor visitor = SQLUtils.createSchemaStatVisitor(JdbcConstants.STARROCKS);
        stmt.accept(visitor);

        System.out.println("Tables : " + visitor.getTables());
        System.out.println("fields : " + visitor.getColumns());
//      System.out.println("coditions : " + visitor.getConditions());
//      System.out.println("orderBy : " + visitor.getOrderByColumns());

        assertEquals(1, visitor.getTables().size());
        assertEquals(10, visitor.getColumns().size());
        assertEquals(2, visitor.getConditions().size());

//        System.out.println(SQLUtils.formatStarrocks(sql));

//        assertTrue(visitor.getColumns().contains(new Column("abc", "name")));
    }

}
