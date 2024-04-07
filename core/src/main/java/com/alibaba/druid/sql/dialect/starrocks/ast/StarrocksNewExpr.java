package com.alibaba.druid.sql.dialect.starrocks.ast;

import com.alibaba.druid.FastsqlException;
import com.alibaba.druid.sql.ast.SQLDataType;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.druid.sql.dialect.starrocks.visitor.StarrocksASTVisitor;
import com.alibaba.druid.sql.visitor.SQLASTVisitor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class StarrocksNewExpr extends SQLMethodInvokeExpr implements StarrocksObject {
    private boolean array;

    private List<SQLExpr> initValues = new ArrayList<>();
    private List<SQLDataType> typeParameters = new ArrayList<>();

    public StarrocksNewExpr() {
    }

    @Override
    public StarrocksNewExpr clone() {
        StarrocksNewExpr x = new StarrocksNewExpr();
        cloneTo(x);
        return x;
    }

    @Override
    public void accept0(SQLASTVisitor v) {
        accept0((StarrocksASTVisitor) v);
    }

    @Override
    public void accept0(StarrocksASTVisitor visitor) {
        if (visitor.visit(this)) {
            if (this.owner != null) {
                this.owner.accept(visitor);
            }

            for (SQLExpr arg : this.arguments) {
                if (arg != null) {
                    arg.accept(visitor);
                }
            }

            if (this.from != null) {
                this.from.accept(visitor);
            }

            if (this.using != null) {
                this.using.accept(visitor);
            }

            if (this.hasFor != null) {
                this.hasFor.accept(visitor);
            }

            visitor.endVisit(this);
        }
        visitor.endVisit(this);
    }

    public void output(StringBuilder buf) {
        try {
            buf.append("new ");
        } catch (Exception ex) {
            throw new FastsqlException("output error", ex);
        }
        super.output(buf);
    }

    public boolean isArray() {
        return array;
    }

    public void setArray(boolean array) {
        this.array = array;
    }

    public List<SQLExpr> getInitValues() {
        return initValues;
    }

    public List<SQLDataType> getTypeParameters() {
        return typeParameters;
    }
}
