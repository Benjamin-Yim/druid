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

import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.dialect.starrocks.visitor.StarrocksASTVisitor;
import com.alibaba.druid.sql.visitor.SQLASTVisitor;

import java.util.ArrayList;

public class StarrocksUDTFSQLSelectItem extends SQLSelectItem implements StarrocksObject {
    public StarrocksUDTFSQLSelectItem() {
        super.aliasList = new ArrayList<String>();
    }

    public void setAlias(String alias) {
        throw new UnsupportedOperationException();
    }

    protected void accept0(SQLASTVisitor visitor) {
        if (visitor instanceof StarrocksASTVisitor) {
            accept0((StarrocksASTVisitor) visitor);
        } else {
            super.accept0(visitor);
        }
    }

    public void accept0(StarrocksASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, this.expr);
        }
        visitor.endVisit(this);
    }

}
