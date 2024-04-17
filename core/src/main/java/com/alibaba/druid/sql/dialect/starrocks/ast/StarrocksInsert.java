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

import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.starrocks.visitor.StarrocksASTVisitor;
import com.alibaba.druid.sql.visitor.SQLASTVisitor;

import java.util.ArrayList;
import java.util.List;

public class StarrocksInsert extends SQLInsertInto {
    public StarrocksInsert() {
        partitions = new ArrayList<SQLAssignItem>();
    }

    public void setPartitions(List<SQLAssignItem> partitions) {
        this.partitions = partitions;
    }

    @Override
    public SQLInsertInto clone() {
        StarrocksInsert x = new StarrocksInsert();
        cloneTo(x);
        return x;
    }

    public SQLExprTableSource getTableSource() {
        return tableSource;
    }

    public void setTableSource(SQLExprTableSource tableSource) {
        if (tableSource != null) {
            tableSource.setParent(this);
        }
        this.tableSource = tableSource;
    }

    public void setTableSource(SQLName tableName) {
        this.setTableSource(new SQLExprTableSource(tableName));
    }

    public SQLSelect getQuery() {
        return query;
    }

    public void setQuery(SQLSelect query) {
        if (query != null) {
            query.setParent(this);
        }
        this.query = query;
    }

    public List<SQLInsertStatement.ValuesClause> getValuesList() {
        return valuesList;
    }

    public void addValueCause(SQLInsertStatement.ValuesClause valueClause) {
        if (valueClause != null) {
            valueClause.setParent(this);
        }
        valuesList.add(valueClause);
    }

    @Override
    protected void accept0(SQLASTVisitor visitor) {
        if (visitor instanceof StarrocksASTVisitor) {
            accept0((StarrocksASTVisitor) visitor);
        } else {
            acceptChild(visitor, tableSource);
            acceptChild(visitor, partitions);
            acceptChild(visitor, valuesList);
            acceptChild(visitor, query);
        }
    }

    protected void accept0(StarrocksASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, tableSource);
            acceptChild(visitor, partitions);
            acceptChild(visitor, valuesList);
            acceptChild(visitor, query);
        }
        visitor.endVisit(this);
    }
}
