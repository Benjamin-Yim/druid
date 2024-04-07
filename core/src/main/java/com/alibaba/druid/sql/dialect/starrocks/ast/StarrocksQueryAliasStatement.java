package com.alibaba.druid.sql.dialect.starrocks.ast;

import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.starrocks.visitor.StarrocksASTVisitor;

public class StarrocksQueryAliasStatement extends StarrocksStatementImpl {
    private String variant;
    private boolean cache;
    private SQLSelectStatement statement;

    public StarrocksQueryAliasStatement() {
    }

    public StarrocksQueryAliasStatement(String variant, SQLSelectStatement statement) {
        this.variant = variant;
        this.statement = statement;
    }

    @Override
    protected void accept0(StarrocksASTVisitor v) {
        if (v.visit(this)) {
            acceptChild(v, statement);
        }
        v.endVisit(this);
    }

    public String getVariant() {
        return variant;
    }

    public void setVariant(String variant) {
        this.variant = variant;
    }

    public SQLSelectStatement getStatement() {
        return statement;
    }

    public void setStatement(SQLSelectStatement statement) {
        this.statement = statement;
    }

    public boolean isCache() {
        return cache;
    }

    public void setCache(boolean cache) {
        this.cache = cache;
    }
}
