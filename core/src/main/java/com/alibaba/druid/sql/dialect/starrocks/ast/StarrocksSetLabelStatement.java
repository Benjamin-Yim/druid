/*
 * Copyright 1999-2017 Alibaba Group Holding Ltd.
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
package com.alibaba.druid.sql.dialect.starrocks.ast;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.druid.sql.dialect.starrocks.visitor.StarrocksASTVisitor;

import java.util.ArrayList;
import java.util.List;

public class StarrocksSetLabelStatement extends StarrocksStatementImpl {
    private String label;

    private SQLExpr project;
    private SQLExpr user;

    private SQLTableSource table;

    private List<SQLName> columns = new ArrayList<SQLName>();

    @Override
    protected void accept0(StarrocksASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, user);
        }
        visitor.endVisit(this);
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public SQLExpr getUser() {
        return user;
    }

    public void setUser(SQLExpr user) {
        this.user = user;
        user.setParent(this);
    }

    public SQLTableSource getTable() {
        return table;
    }

    public void setTable(SQLTableSource table) {
        this.table = table;
        table.setParent(this);
    }

    public SQLExpr getProject() {
        return project;
    }

    public void setProject(SQLExpr x) {
        if (x != null) {
            x.setParent(this);
        }
        this.project = x;
    }

    public List<SQLName> getColumns() {
        return columns;
    }

}
