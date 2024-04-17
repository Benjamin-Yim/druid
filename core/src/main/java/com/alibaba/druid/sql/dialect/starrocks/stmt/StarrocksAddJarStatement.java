package com.alibaba.druid.sql.dialect.starrocks.stmt;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.ast.SQLStatementImpl;
import com.alibaba.druid.sql.visitor.SQLASTVisitor;

public class StarrocksAddJarStatement extends SQLStatementImpl {
    public StarrocksAddJarStatement() {
        this.dbType = DbType.starrocks;
    }

    private String path;

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    @Override
    protected void accept0(SQLASTVisitor visitor) {
        if (visitor instanceof StarrocksASTVisitor) {
            accept0((StarrocksASTVisitor) visitor);
        }
    }

    protected void accept0(StarrocksASTVisitor v) {
        v.visit(this);
        v.endVisit(this);
    }
}
