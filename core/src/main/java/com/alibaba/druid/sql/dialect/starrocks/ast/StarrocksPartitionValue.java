package com.alibaba.druid.sql.dialect.starrocks.ast;

import com.alibaba.druid.sql.ast.SQLPartitionValue;

public class StarrocksPartitionValue extends SQLPartitionValue {
    public StarrocksPartitionValue(Operator operator) {
        super(operator);
    }

    @Override
    public String toString() {
        if (this.operator == Operator.Between_and && super.getItems().size() > 0) {
            String start = super.getItems().get(0).toString();
            String end = super.getItems().get(super.getItems().size() - 1).toString();
            StringBuilder builder = new StringBuilder("BETWEEN");
            builder.append(" ");
            builder.append(start);
            builder.append(" ");
            builder.append("AND");
            builder.append(" ");
            builder.append(end);

            return builder.toString();
        }
        return super.toString();
    }
}
