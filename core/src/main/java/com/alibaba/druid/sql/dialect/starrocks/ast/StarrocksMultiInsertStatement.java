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
import com.alibaba.druid.sql.ast.SQLStatementImpl;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.druid.sql.ast.statement.SQLWithSubqueryClause;
import com.alibaba.druid.sql.dialect.starrocks.visitor.StarrocksASTVisitor;
import com.alibaba.druid.sql.visitor.SQLASTVisitor;

import java.util.ArrayList;
import java.util.List;

public class StarrocksMultiInsertStatement extends SQLStatementImpl {
    protected SQLWithSubqueryClause with;
    private SQLTableSource from;

    private List<StarrocksInsert> items = new ArrayList<StarrocksInsert>();

    public StarrocksMultiInsertStatement() {
        super(DbType.starrocks);
    }

    public void setFrom(SQLTableSource x) {
        if (x != null) {
            x.setParent(this);
        }

        this.from = x;
    }

    public SQLTableSource getFrom() {
        return from;
    }

    public List<StarrocksInsert> getItems() {
        return items;
    }

    public void addItem(StarrocksInsert item) {
        if (item != null) {
            item.setParent(this);
        }
        this.items.add(item);
    }

    @Override
    protected void accept0(SQLASTVisitor visitor) {
        if (visitor instanceof StarrocksASTVisitor) {
            accept0((StarrocksASTVisitor) visitor);
        } else {
            acceptChild(visitor, with);
            acceptChild(visitor, from);
            acceptChild(visitor, items);
        }
    }

    public void accept0(StarrocksASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, with);
            acceptChild(visitor, from);
            acceptChild(visitor, items);
        }
        visitor.endVisit(this);
    }

    public SQLWithSubqueryClause getWith() {
        return with;
    }

    public void setWith(SQLWithSubqueryClause with) {
        if (with != null) {
            with.setParent(this);
        }
        this.with = with;
    }
}
