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

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.statement.SQLAlterStatement;
import com.alibaba.druid.sql.ast.statement.SQLAssignItem;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.dialect.starrocks.visitor.StarrocksASTVisitor;

import java.util.ArrayList;
import java.util.List;

public class StarrocksRestoreStatement
        extends StarrocksStatementImpl implements SQLAlterStatement {
    private final List<SQLAssignItem> properties = new ArrayList<SQLAssignItem>();
    private final List<SQLAssignItem> partitions = new ArrayList<SQLAssignItem>();
    private SQLExprTableSource table;
    private SQLExpr to;

    private String alias;

    public StarrocksRestoreStatement() {
        super.dbType = DbType.starrocks;
    }

    @Override
    protected void accept0(StarrocksASTVisitor visitor) {
        if (visitor.visit(this)) {
            this.acceptChild(visitor, table);
        }
        visitor.endVisit(this);
    }

    public SQLExprTableSource getTable() {
        return table;
    }

    public void setTable(SQLExprTableSource table) {
        if (table != null) {
            table.setParent(table);
        }
        this.table = table;
    }

    public void setTable(SQLName table) {
        this.setTable(new SQLExprTableSource(table));
    }

    public List<SQLAssignItem> getProperties() {
        return properties;
    }

    public List<SQLAssignItem> getPartitions() {
        return partitions;
    }

    public SQLExpr getTo() {
        return to;
    }

    public void setTo(SQLExpr x) {
        if (x != null) {
            x.setParent(this);
        }
        this.to = x;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }
}
