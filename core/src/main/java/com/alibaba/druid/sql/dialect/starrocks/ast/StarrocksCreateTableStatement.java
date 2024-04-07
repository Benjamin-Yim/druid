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
import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.dialect.hive.stmt.HiveCreateTableStatement;
import com.alibaba.druid.sql.dialect.starrocks.visitor.StarrocksASTVisitor;
import com.alibaba.druid.sql.visitor.SQLASTVisitor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StarrocksCreateTableStatement extends HiveCreateTableStatement {
    protected final List<SQLExpr> withSerdeproperties = new ArrayList<SQLExpr>();
    protected SQLExpr lifecycle;
    protected SQLExpr storedBy;
    public List<SQLName> distributeBy = new ArrayList<>();
    public Map<String, String> properties = new HashMap<>();

    // for ads
    protected SQLName distributeByType;

    public StarrocksCreateTableStatement() {
        super(DbType.starrocks);
    }

    public SQLExprTableSource getLike() {
        return like;
    }

    public void setLike(SQLName like) {
        this.setLike(new SQLExprTableSource(like));
    }

    public void setLike(SQLExprTableSource like) {
        this.like = like;
    }

    public List<SQLColumnDefinition> getPartitionColumns() {
        return partitionColumns;
    }

    public void addPartitionColumn(SQLColumnDefinition column) {
        if (column != null) {
            column.setParent(this);
        }
        this.partitionColumns.add(column);
    }

    public SQLExpr getLifecycle() {
        return lifecycle;
    }

    public void setLifecycle(SQLExpr lifecycle) {
        this.lifecycle = lifecycle;
    }

    @Override
    protected void accept0(SQLASTVisitor v) {
        if (v instanceof StarrocksASTVisitor) {
            accept0((StarrocksASTVisitor) v);
            return;
        }

        super.accept0(v);
    }

    protected void accept0(StarrocksASTVisitor v) {
        if (v.visit(this)) {
            acceptChild(v);
        }
        v.endVisit(this);
    }

    protected void acceptChild(SQLASTVisitor v) {
        super.acceptChild(v);

        acceptChild(v, withSerdeproperties);
        acceptChild(v, lifecycle);
        acceptChild(v, storedBy);
    }

    public SQLExpr getStoredBy() {
        return storedBy;
    }

    public void setStoredBy(SQLExpr x) {
        if (x != null) {
            x.setParent(this);
        }
        this.storedBy = x;
    }

    public List<SQLExpr> getWithSerdeproperties() {
        return withSerdeproperties;
    }

    public List<SQLName> getDistributeBy() {
        return distributeBy;
    }

    public void setDistributeBy(List<SQLName> distributeBy) {
        this.distributeBy = distributeBy;
    }

    public SQLName getDistributeByType() {
        return distributeByType;
    }

    public void setDistributeByType(SQLName distributeByType) {
        this.distributeByType = distributeByType;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }
}
