package com.alibaba.druid.sql.dialect.starrocks.ast;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableItem;
import com.alibaba.druid.sql.dialect.starrocks.visitor.StarrocksASTVisitor;

public class StarrocksAlterTableSetChangeLogs extends StarrocksObjectImpl
        implements SQLAlterTableItem {
    private SQLExpr value;

    @Override
    public void accept0(StarrocksASTVisitor v) {
        if (v.visit(this)) {
            acceptChild(v, value);
        }
        v.endVisit(this);
    }

    public SQLExpr getValue() {
        return value;
    }

    public void setValue(SQLExpr x) {
        if (x != null) {
            x.setParent(this);
        }
        this.value = x;
    }
}
