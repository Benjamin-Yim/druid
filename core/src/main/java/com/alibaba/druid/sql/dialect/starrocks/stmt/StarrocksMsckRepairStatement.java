package com.alibaba.druid.sql.dialect.starrocks.stmt;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.SQLStatementImpl;
import com.alibaba.druid.sql.ast.statement.SQLAlterStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.visitor.SQLASTVisitor;

public class StarrocksMsckRepairStatement extends SQLStatementImpl implements SQLAlterStatement {
    private SQLExprTableSource table;
    private SQLName database;
    private boolean addPartitions;

    public StarrocksMsckRepairStatement() {
        super(DbType.starrocks);
    }

    protected void accept0(SQLASTVisitor visitor) {
        if (visitor instanceof StarrocksASTVisitor) {
            accept0((StarrocksASTVisitor) visitor);
        } else {
            super.accept0(visitor);
        }
    }

    protected void accept0(StarrocksASTVisitor visitor) {
        if (visitor.visit(this)) {
            this.acceptChild(visitor, table);
            this.acceptChild(visitor, database);
        }
        visitor.endVisit(this);
    }

    public boolean isAddPartitions() {
        return addPartitions;
    }

    public void setAddPartitions(boolean addPartitions) {
        this.addPartitions = addPartitions;
    }

    public SQLExprTableSource getTable() {
        return table;
    }

    public void setTable(SQLExprTableSource x) {
        if (x != null) {
            x.setParent(this);
        }
        this.table = x;
    }

    public void setTable(SQLExpr x) {
        if (x == null) {
            this.table = null;
            return;
        }

        setTable(new SQLExprTableSource(x));
    }

    public SQLName getDatabase() {
        return database;
    }

    public void setDatabase(SQLName x) {
        if (x != null) {
            x.setParent(this);
        }
        this.database = x;
    }
}
