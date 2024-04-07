package com.alibaba.druid.sql.dialect.starrocks.ast;

import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.dialect.starrocks.visitor.StarrocksASTVisitor;

public class StarrocksInstallPackageStatement extends StarrocksStatementImpl {
    private SQLName packageName;

    @Override
    protected void accept0(StarrocksASTVisitor v) {
        if (v.visit(this)) {
            acceptChild(v, packageName);
        }
        v.endVisit(this);
    }

    public SQLName getPackageName() {
        return packageName;
    }

    public void setPackageName(SQLName x) {
        if (x != null) {
            x.setParent(this);
        }
        this.packageName = x;
    }
}
