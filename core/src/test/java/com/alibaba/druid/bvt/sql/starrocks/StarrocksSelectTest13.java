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
import org.junit.Assert;

import java.util.List;

public class StarrocksSelectTest13 extends TestCase {
    public void test_select() throws Exception {
        String sql = "select `name` from `abc`;";//
        Assert.assertEquals("SELECT `name`"
                + "\nFROM `abc`;", SQLUtils.formatStarrocks(sql));
        Assert.assertEquals("select `name`"
                + "\nfrom `abc`;", SQLUtils.formatStarrocks(sql, SQLUtils.DEFAULT_LCASE_FORMAT_OPTION));

        List<SQLStatement> statementList = SQLUtils.parseStatements(sql, JdbcConstants.STARROCKS);
        SQLStatement stmt = statementList.get(0);

        Assert.assertEquals(1, statementList.size());

        SchemaStatVisitor visitor = SQLUtils.createSchemaStatVisitor(JdbcConstants.STARROCKS);
        stmt.accept(visitor);

//        System.out.println("Tables : " + visitor.getTables());
        System.out.println("fields : " + visitor.getColumns());
//      System.out.println("coditions : " + visitor.getConditions());
//      System.out.println("orderBy : " + visitor.getOrderByColumns());

        Assert.assertEquals(1, visitor.getTables().size());
        Assert.assertEquals(1, visitor.getColumns().size());
        Assert.assertEquals(0, visitor.getConditions().size());

        assertTrue(visitor.containsColumn("abc", "name"));
    }

}
