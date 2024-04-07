package com.alibaba.druid.sql.dialect.starrocks.ast;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLExprImpl;
import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.druid.sql.ast.statement.SQLExternalRecordFormat;
import com.alibaba.druid.sql.dialect.starrocks.visitor.StarrocksASTVisitor;
import com.alibaba.druid.sql.visitor.SQLASTVisitor;

import java.util.ArrayList;
import java.util.List;

public class StarrocksTransformExpr extends SQLExprImpl implements StarrocksObject {
    private SQLExternalRecordFormat inputRowFormat;
    private final List<SQLExpr> inputColumns = new ArrayList<>();
    private final List<SQLColumnDefinition> outputColumns = new ArrayList<>();
    private SQLExpr using;
    private final List<SQLExpr> resources = new ArrayList<>();
    private SQLExternalRecordFormat outputRowFormat;

    @Override
    public boolean equals(Object o) {
        return false;
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    protected void accept0(SQLASTVisitor v) {
        accept0((StarrocksASTVisitor) v);
    }

    @Override
    public void accept0(StarrocksASTVisitor v) {
        if (v.visit(this)) {
            acceptChild(v, inputColumns);
            acceptChild(v, outputColumns);
        }
        v.endVisit(this);
    }

    @Override
    public SQLExpr clone() {
        return null;
    }

    public SQLExternalRecordFormat getInputRowFormat() {
        return inputRowFormat;
    }

    public void setInputRowFormat(SQLExternalRecordFormat inputRowFormat) {
        this.inputRowFormat = inputRowFormat;
    }

    public List<SQLExpr> getInputColumns() {
        return inputColumns;
    }

    public List<SQLColumnDefinition> getOutputColumns() {
        return outputColumns;
    }

    public SQLExpr getUsing() {
        return using;
    }

    public void setUsing(SQLExpr using) {
        this.using = using;
    }

    public SQLExternalRecordFormat getOutputRowFormat() {
        return outputRowFormat;
    }

    public void setOutputRowFormat(SQLExternalRecordFormat outputRowFormat) {
        this.outputRowFormat = outputRowFormat;
    }

    public List<SQLExpr> getResources() {
        return resources;
    }
}
