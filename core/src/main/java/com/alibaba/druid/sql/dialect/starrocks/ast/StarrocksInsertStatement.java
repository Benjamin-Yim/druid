package com.alibaba.druid.sql.dialect.starrocks.ast;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.ast.SQLCommentHint;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLAssignItem;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.dialect.starrocks.visitor.StarrocksASTVisitor;
import com.alibaba.druid.sql.visitor.SQLASTVisitor;

import java.util.ArrayList;
import java.util.List;

public class StarrocksInsertStatement extends SQLInsertStatement implements SQLStatement {
    private boolean ifNotExists;

    public StarrocksInsertStatement() {
        dbType = DbType.starrocks;
        partitions = new ArrayList<SQLAssignItem>();
    }

    public StarrocksInsertStatement clone() {
        StarrocksInsertStatement x = new StarrocksInsertStatement();
        super.cloneTo(x);
        return x;
    }

    @Override
    protected void accept0(SQLASTVisitor visitor) {
        if (visitor instanceof StarrocksASTVisitor) {
            accept0((StarrocksASTVisitor) visitor);
        } else {
            super.accept0(visitor);
        }
    }

    protected void accept0(StarrocksASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, with);
            acceptChild(visitor, tableSource);
            acceptChild(visitor, partitions);
            acceptChild(visitor, valuesList);
            acceptChild(visitor, query);
        }
        visitor.endVisit(this);
    }


    @Override
    public List<SQLCommentHint> getHeadHintsDirect() {
        return null;
    }

    public boolean isIfNotExists() {
        return ifNotExists;
    }

    public void setIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }
}
