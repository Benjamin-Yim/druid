/*
 * Copyright 1999-2017 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") {
        return true;
    }
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
package com.alibaba.druid.sql.dialect.starrocks.visitor;

import com.alibaba.druid.sql.ast.statement.SQLCreateTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLGrantStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.druid.sql.dialect.hive.visitor.HiveASTVisitor;
import com.alibaba.druid.sql.dialect.starrocks.ast.*;
import com.alibaba.druid.sql.visitor.SQLASTVisitor;

public interface StarrocksASTVisitor extends SQLASTVisitor {
    default void endVisit(StarrocksCreateTableStatement x) {
        endVisit((SQLCreateTableStatement) x);
    }

    default boolean visit(StarrocksCreateTableStatement x) {
        return visit((SQLCreateTableStatement) x);
    }

    default void endVisit(StarrocksUDTFSQLSelectItem x) {
    }

    default boolean visit(StarrocksUDTFSQLSelectItem x) {
        return true;
    }

    default void endVisit(StarrocksSetLabelStatement x) {
    }

    default boolean visit(StarrocksSetLabelStatement x) {
        return true;
    }

    default void endVisit(StarrocksSelectQueryBlock x) {
        endVisit((SQLSelectQueryBlock) x);
    }

    default boolean visit(StarrocksSelectQueryBlock x) {
        return visit((SQLSelectQueryBlock) x);
    }

    default void endVisit(StarrocksAddStatisticStatement x) {
    }

    default boolean visit(StarrocksAddStatisticStatement x) {
        return true;
    }

    default void endVisit(StarrocksRemoveStatisticStatement x) {
    }

    default boolean visit(StarrocksRemoveStatisticStatement x) {
        return true;
    }

    default void endVisit(StarrocksStatisticClause.TableCount x) {
    }

    default boolean visit(StarrocksStatisticClause.TableCount x) {
        return true;
    }

    default void endVisit(StarrocksStatisticClause.ExpressionCondition x) {
    }

    default boolean visit(StarrocksStatisticClause.ExpressionCondition x) {
        return true;
    }

    default void endVisit(StarrocksStatisticClause.NullValue x) {
    }

    default boolean visit(StarrocksStatisticClause.NullValue x) {
        return true;
    }

    default void endVisit(StarrocksStatisticClause.DistinctValue x) {
    }

    default boolean visit(StarrocksStatisticClause.DistinctValue x) {
        return true;
    }

    default void endVisit(StarrocksStatisticClause.ColumnSum x) {
    }

    default boolean visit(StarrocksStatisticClause.ColumnSum x) {
        return true;
    }

    default void endVisit(StarrocksStatisticClause.ColumnMax x) {
    }

    default boolean visit(StarrocksStatisticClause.ColumnMax x) {
        return true;
    }

    default void endVisit(StarrocksStatisticClause.ColumnMin x) {
    }

    default boolean visit(StarrocksStatisticClause.ColumnMin x) {
        return true;
    }

    default void endVisit(StarrocksReadStatement x) {
    }

    default boolean visit(StarrocksReadStatement x) {
        return true;
    }

    default void endVisit(StarrocksShowGrantsStmt x) {
    }

    default boolean visit(StarrocksShowGrantsStmt x) {
        return true;
    }

    default void endVisit(StarrocksShowChangelogsStatement x) {
    }

    default boolean visit(StarrocksShowChangelogsStatement x) {
        return true;
    }

    default void endVisit(StarrocksListStmt x) {
    }

    default boolean visit(StarrocksListStmt x) {
        return true;
    }

    default void endVisit(StarrocksGrantStmt x) {
        endVisit((SQLGrantStatement) x);
    }

    default boolean visit(StarrocksGrantStmt x) {
        return visit((SQLGrantStatement) x);
    }

    default boolean visit(StarrocksAddTableStatement x) {
        return true;
    }

    default void endVisit(StarrocksAddTableStatement x) {
    }

    default boolean visit(StarrocksAddFileStatement x) {
        return true;
    }

    default void endVisit(StarrocksAddFileStatement x) {
    }

    default boolean visit(StarrocksAddUserStatement x) {
        return true;
    }

    default void endVisit(StarrocksAddUserStatement x) {
    }

    default boolean visit(StarrocksRemoveUserStatement x) {
        return true;
    }

    default void endVisit(StarrocksRemoveUserStatement x) {
    }

    default boolean visit(StarrocksAlterTableSetChangeLogs x) {
        return true;
    }

    default void endVisit(StarrocksAlterTableSetChangeLogs x) {
    }

    default boolean visit(StarrocksCountStatement x) {
        return true;
    }

    default void endVisit(StarrocksCountStatement x) {
    }

    default boolean visit(StarrocksQueryAliasStatement x) {
        return true;
    }

    default void endVisit(StarrocksQueryAliasStatement x) {
    }

    default boolean visit(StarrocksTransformExpr x) {
        return true;
    }

    default void endVisit(StarrocksTransformExpr x) {
    }

    default boolean visit(StarrocksExstoreStatement x) {
        return true;
    }

    default void endVisit(StarrocksExstoreStatement x) {
    }

    default boolean visit(StarrocksNewExpr x) {
        return true;
    }

    default void endVisit(StarrocksNewExpr x) {
    }

    default boolean visit(StarrocksInstallPackageStatement x) {
        return true;
    }

    default void endVisit(StarrocksInstallPackageStatement x) {
    }

    default boolean visit(StarrocksDeclareVariableStatement x) {
        return true;
    }

    default void endVisit(StarrocksDeclareVariableStatement x) {
    }

    default boolean visit(StarrocksAddAccountProviderStatement x) {
        return true;
    }

    default void endVisit(StarrocksAddAccountProviderStatement x) {
    }

    default boolean visit(StarrocksUnloadStatement x) {
        return true;
    }

    default void endVisit(StarrocksUnloadStatement x) {
    }

    default boolean visit(StarrocksAlterTableSetFileFormat x) {
        return true;
    }

    default void endVisit(StarrocksAlterTableSetFileFormat x) {
    }

    default boolean visit(StarrocksRestoreStatement x) {
        return true;
    }

    default void endVisit(StarrocksRestoreStatement x) {
    }

    default boolean visit(StarrocksUndoTableStatement x) {
        return true;
    }

    default void endVisit(StarrocksUndoTableStatement x) {
    }

}
