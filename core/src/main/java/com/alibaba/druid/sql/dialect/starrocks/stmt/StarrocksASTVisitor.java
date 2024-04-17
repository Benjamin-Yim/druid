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
package com.alibaba.druid.sql.dialect.starrocks.stmt;

import com.alibaba.druid.sql.dialect.starrocks.ast.StarrocksInsert;
import com.alibaba.druid.sql.dialect.starrocks.ast.StarrocksInsertStatement;
import com.alibaba.druid.sql.dialect.starrocks.ast.StarrocksMultiInsertStatement;
import com.alibaba.druid.sql.visitor.SQLASTVisitor;

public interface StarrocksASTVisitor extends SQLASTVisitor {
    default boolean visit(StarrocksInsert x) {
        return true;
    }

    default void endVisit(StarrocksInsert x) {
    }

    default boolean visit(StarrocksMultiInsertStatement x) {
        return true;
    }

    default void endVisit(StarrocksMultiInsertStatement x) {
    }

    default boolean visit(StarrocksInsertStatement x) {
        return true;
    }

    default void endVisit(StarrocksInsertStatement x) {
    }

    default boolean visit(StarrocksCreateFunctionStatement x) {
        return true;
    }

    default void endVisit(StarrocksCreateFunctionStatement x) {
    }

    default boolean visit(StarrocksLoadDataStatement x) {
        return true;
    }

    default void endVisit(StarrocksLoadDataStatement x) {
    }

    default boolean visit(StarrocksMsckRepairStatement x) {
        return true;
    }

    default void endVisit(StarrocksMsckRepairStatement x) {
    }

    default boolean visit(StarrocksAddJarStatement x) {
        return true;
    }

    default void endVisit(StarrocksAddJarStatement x) {
    }
}
