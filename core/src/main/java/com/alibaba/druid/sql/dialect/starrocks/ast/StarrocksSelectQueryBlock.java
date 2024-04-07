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
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLLimit;
import com.alibaba.druid.sql.ast.SQLZOrderBy;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelectOrderByItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.druid.sql.dialect.starrocks.visitor.StarrocksASTVisitor;
import com.alibaba.druid.sql.visitor.SQLASTVisitor;

import java.util.ArrayList;

public class StarrocksSelectQueryBlock extends SQLSelectQueryBlock {
    private SQLZOrderBy zOrderBy;

    public StarrocksSelectQueryBlock() {
        dbType = DbType.starrocks;

        clusterBy = new ArrayList<SQLSelectOrderByItem>();
        distributeBy = new ArrayList<SQLSelectOrderByItem>();
        sortBy = new ArrayList<SQLSelectOrderByItem>(2);
    }

    public StarrocksSelectQueryBlock clone() {
        StarrocksSelectQueryBlock x = new StarrocksSelectQueryBlock();
        cloneTo(x);
        return x;
    }

    @Override
    protected void accept0(SQLASTVisitor visitor) {
        if (visitor instanceof StarrocksASTVisitor) {
            accept0((StarrocksASTVisitor) visitor);
            return;
        }

        super.accept0(visitor);
    }

    public void accept0(StarrocksASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, this.hints);
            acceptChild(visitor, this.selectList);
            acceptChild(visitor, this.from);
            acceptChild(visitor, this.where);
            acceptChild(visitor, this.groupBy);
            acceptChild(visitor, this.orderBy);
            acceptChild(visitor, this.zOrderBy);
            acceptChild(visitor, this.clusterBy);
            acceptChild(visitor, this.distributeBy);
            acceptChild(visitor, this.sortBy);
            acceptChild(visitor, this.limit);
            acceptChild(visitor, this.into);
        }

        visitor.endVisit(this);
    }

    public String toString() {
        return SQLUtils.toStarrocksString(this);
    }

    public void limit(int rowCount, int offset) {
        if (offset > 0) {
            throw new UnsupportedOperationException("not support offset");
        }

        setLimit(new SQLLimit(new SQLIntegerExpr(rowCount)));
    }

    public SQLZOrderBy getZOrderBy() {
        return zOrderBy;
    }

    public void setZOrderBy(SQLZOrderBy x) {
        if (x != null) {
            x.setParent(this);
        }

        this.zOrderBy = x;
    }
}
