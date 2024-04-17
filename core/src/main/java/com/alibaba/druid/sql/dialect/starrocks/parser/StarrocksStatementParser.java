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
package com.alibaba.druid.sql.dialect.starrocks.parser;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.ast.*;
import com.alibaba.druid.sql.ast.expr.SQLAllColumnExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLQueryExpr;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.starrocks.stmt.StarrocksCreateFunctionStatement;
import com.alibaba.druid.sql.dialect.starrocks.ast.StarrocksInsert;
import com.alibaba.druid.sql.dialect.starrocks.ast.StarrocksInsertStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlKillStatement;
import com.alibaba.druid.sql.dialect.starrocks.ast.*;
import com.alibaba.druid.sql.dialect.starrocks.stmt.StarrocksLoadDataStatement;
import com.alibaba.druid.sql.parser.*;
import com.alibaba.druid.util.FnvHash;

import java.util.List;

import static com.alibaba.druid.sql.parser.Token.*;
import static com.alibaba.druid.sql.parser.Token.RPAREN;

public class StarrocksStatementParser extends SQLStatementParser {
    public StarrocksStatementParser(String sql) {
        super(new StarrocksExprParser(sql));
    }

    public StarrocksStatementParser(String sql, SQLParserFeature... features) {
        super(new StarrocksExprParser(sql, features));
    }

    public StarrocksStatementParser(SQLExprParser exprParser) {
        super(exprParser);
    }

    public SQLSelectStatement parseSelect() {
        SQLSelect select = new StarrocksSelectParser(this.exprParser)
                .select();

//        if (select.getWithSubQuery() == null && select.getQuery() instanceof SQLSelectQueryBlock) {
//            SQLSelectQueryBlock queryBlock = (SQLSelectQueryBlock) select.getQuery();
//            if (queryBlock.getFrom() == null && queryBlock.getWhere() != null) {
//                throw new ParserException("none from query not support where clause.");
//            }
//        }

        return new SQLSelectStatement(select, DbType.starrocks);
    }

    public SQLCreateTableStatement parseCreateTable() {
        SQLCreateTableParser parser = new StarrocksCreateTableParser(this.exprParser);
        return parser.parseCreateTable();
    }

    public SQLCreateTableParser getSQLCreateTableParser() {
        return new StarrocksCreateTableParser(this.exprParser);
    }


    public SQLCreateFunctionStatement parseStarrocksCreateFunction() {
        StarrocksCreateFunctionStatement stmt = new StarrocksCreateFunctionStatement();
        stmt.setDbType(dbType);

        if (lexer.token() == CREATE) {
            lexer.nextToken();
        }

        if (lexer.token() == OR) {
            lexer.nextToken();
            accept(REPLACE);
            stmt.setOrReplace(true);
        }

        if (lexer.identifierEquals(FnvHash.Constants.TEMPORARY)) {
            lexer.nextToken();
            stmt.setTemporary(true);
        }

        boolean sql = false;
        if (lexer.identifierEquals(FnvHash.Constants.SQL)) {
            lexer.nextToken();
            sql = true;
        }

        accept(Token.FUNCTION);

        if (lexer.token() == IF) {
            lexer.nextToken();
            accept(NOT);
            accept(EXISTS);
            stmt.setIfNotExists(true);
        }

        SQLName name = this.exprParser.name();
        stmt.setName(name);

        if (lexer.token() == LPAREN) {
            lexer.nextToken();
            while (lexer.token() != RPAREN) {
                SQLParameter param = new SQLParameter();
                param.setName(this.exprParser.name());
                param.setDataType(this.exprParser.parseDataType());
                if (lexer.token() == COMMA) {
                    lexer.nextToken();
                }
                stmt.getParameters().add(param);
                param.setParent(stmt);
            }
            accept(RPAREN);
        }

        if (lexer.identifierEquals(FnvHash.Constants.RETURNS)) {
            lexer.nextToken();
            if (lexer.token() == VARIANT) {
                lexer.nextToken(); // TODO
            }
            SQLDataType returnDataType = this.exprParser.parseDataType();
            stmt.setReturnDataType(returnDataType);
        }

        if (lexer.token() == IDENTIFIER && lexer.stringVal().toUpperCase().startsWith("RETURNS@")) {
            lexer.nextToken();
            SQLDataType returnDataType = this.exprParser.parseDataType();
            stmt.setReturnDataType(returnDataType);
        }

        if (lexer.token() == Token.AS) {
            lexer.setToken(Token.IDENTIFIER);
            lexer.nextToken();
            if (lexer.token() != BEGIN && !lexer.identifierEquals(FnvHash.Constants.BEGIN)) {
                SQLExpr className = this.exprParser.expr();
                stmt.setClassName(className);
            }
        }

        if (lexer.identifierEquals(FnvHash.Constants.LOCATION)) {
            lexer.nextToken();
            SQLExpr location = this.exprParser.primary();
            stmt.setLocation(location);
        }

        if (lexer.identifierEquals(FnvHash.Constants.SYMBOL)) {
            lexer.nextToken();
            accept(Token.EQ);
            SQLExpr symbol = this.exprParser.primary();
            stmt.setSymbol(symbol);
        }

        if (lexer.token() == Token.USING || lexer.hashLCase() == FnvHash.Constants.USING) {
            lexer.nextToken();

            if (lexer.identifierEquals(FnvHash.Constants.JAR)) {
                lexer.nextToken();
                stmt.setResourceType(StarrocksCreateFunctionStatement.ResourceType.JAR);
            } else if (lexer.identifierEquals(FnvHash.Constants.ARCHIVE)) {
                lexer.nextToken();
                stmt.setResourceType(StarrocksCreateFunctionStatement.ResourceType.ARCHIVE);
            } else if (lexer.identifierEquals(FnvHash.Constants.FILE)) {
                lexer.nextToken();
                stmt.setResourceType(StarrocksCreateFunctionStatement.ResourceType.FILE);
            } else if (lexer.token() == Token.CODE) {
                stmt.setCode(lexer.stringVal());
                lexer.nextToken();
                stmt.setResourceType(StarrocksCreateFunctionStatement.ResourceType.CODE);
                return stmt;
            }

            SQLExpr location = this.exprParser.primary();
            stmt.setLocation(location);
        }

        return stmt;
    }


    public boolean parseStatementListDialect(List<SQLStatement> statementList) {
        if (lexer.token() == Token.FROM) {
            SQLStatement stmt = this.parseInsert();
            statementList.add(stmt);
            return true;
        }

        if (lexer.identifierEquals("ANALYZE")) {
            SQLStatement stmt = parseAnalyze();
            statementList.add(stmt);
            return true;
        }

        if (lexer.identifierEquals("ADD")) {
            lexer.nextToken();

            if (lexer.identifierEquals("STATISTIC")) {
                lexer.nextToken();
                StarrocksAddStatisticStatement stmt = new StarrocksAddStatisticStatement();
                stmt.setTable(this.exprParser.name());
                stmt.setStatisticClause(parseStaticClause());
                statementList.add(stmt);
                return true;
            }

            if (lexer.token() == Token.USER) {
                lexer.nextToken();
                StarrocksAddUserStatement stmt = new StarrocksAddUserStatement();
                stmt.setUser(this.exprParser.name());
                statementList.add(stmt);
                return true;
            }

            if (lexer.identifierEquals("ACCOUNTPROVIDER")) {
                lexer.nextToken();
                StarrocksAddAccountProviderStatement stmt = new StarrocksAddAccountProviderStatement();
                stmt.setProvider(this.exprParser.name());
                statementList.add(stmt);
                return true;
            }

            if (lexer.token() == Token.TABLE) {
                lexer.nextToken();
                StarrocksAddTableStatement stmt = new StarrocksAddTableStatement();
                stmt.setTable(this.exprParser.name());

                if (lexer.token() == Token.PARTITION) {
                    lexer.nextToken();
                    this.exprParser.parseAssignItem(stmt.getPartitions(), stmt);
                }

                if (lexer.token() == Token.AS) {
                    lexer.nextToken();
                    SQLName name = this.exprParser.name();
                    stmt.getTable().setAlias(name.toString());
                }

                if (lexer.token() == Token.COMMENT) {
                    lexer.nextToken();
                    stmt.setComment(this.exprParser.primary());
                }

                if (lexer.token() == Token.SUB) {
                    lexer.nextToken();
                    acceptIdentifier("f");
                    stmt.setForce(true);
                }

                if (lexer.token() == Token.TO) {
                    lexer.nextToken();
                    acceptIdentifier("PACKAGE");
                    SQLName packageName = this.exprParser.name();
                    stmt.setToPackage(packageName);

                    if (lexer.token() == Token.WITH) {
                        lexer.nextToken();
                        acceptIdentifier("PRIVILEGES");
                        parsePrivileages(stmt.getPrivileges(), stmt);
                    }
                }

                statementList.add(stmt);
                return true;
            }

            if (lexer.identifierEquals(FnvHash.Constants.FILE)
                    || lexer.identifierEquals(FnvHash.Constants.JAR)
                    || lexer.identifierEquals(FnvHash.Constants.PY)
                    || lexer.identifierEquals(FnvHash.Constants.ARCHIVE)) {
                StarrocksAddFileStatement stmt = new StarrocksAddFileStatement();

                long hash = lexer.hashLCase();
                if (hash == FnvHash.Constants.JAR) {
                    stmt.setType(StarrocksAddFileStatement.FileType.JAR);
                } else if (hash == FnvHash.Constants.PY) {
                    stmt.setType(StarrocksAddFileStatement.FileType.PY);
                } else if (hash == FnvHash.Constants.ARCHIVE) {
                    stmt.setType(StarrocksAddFileStatement.FileType.ARCHIVE);
                }

                lexer.nextPath();
                String path = lexer.stringVal();

                lexer.nextToken();

                stmt.setFile(path);

                if (lexer.token() == Token.AS) {
                    lexer.nextToken();
                    SQLName name = this.exprParser.name();
                    stmt.setAlias(name.toString());
                }

                if (lexer.token() == Token.COMMENT) {
                    lexer.nextToken();
                    stmt.setComment(this.exprParser.primary());
                }

                if (lexer.token() == Token.SUB) {
                    lexer.nextToken();
                    acceptIdentifier("f");
                    stmt.setForce(true);
                }
                statementList.add(stmt);
                return true;
            }

            throw new ParserException("TODO " + lexer.info());
        }

        if (lexer.identifierEquals("REMOVE")) {
            lexer.nextToken();

            if (lexer.identifierEquals("STATISTIC")) {
                lexer.nextToken();
                StarrocksRemoveStatisticStatement stmt = new StarrocksRemoveStatisticStatement();
                stmt.setTable(this.exprParser.name());
                stmt.setStatisticClause(parseStaticClause());
                statementList.add(stmt);
                return true;
            }

            if (lexer.token() == Token.USER) {
                lexer.nextToken();
                StarrocksRemoveUserStatement stmt = new StarrocksRemoveUserStatement();
                stmt.setUser((SQLIdentifierExpr) this.exprParser.name());
                statementList.add(stmt);
                return true;
            }

            throw new ParserException("TODO " + lexer.info());
        }

        if (lexer.identifierEquals("READ")) {
            StarrocksReadStatement stmt = new StarrocksReadStatement();

            if (lexer.hasComment() && lexer.isKeepComments()) {
                stmt.addBeforeComment(lexer.readAndResetComments());
            }
            lexer.nextToken();

            stmt.setTable(this.exprParser.name());

            if (lexer.token() == Token.LPAREN) {
                lexer.nextToken();
                this.exprParser.names(stmt.getColumns(), stmt);
                accept(Token.RPAREN);
            }

            if (lexer.token() == Token.PARTITION) {
                lexer.nextToken();

                accept(Token.LPAREN);
                parseAssignItems(stmt.getPartition(), stmt);
                accept(Token.RPAREN);
            }

            if (lexer.token() == Token.LITERAL_INT) {
                stmt.setRowCount(this.exprParser.primary());
            }

            statementList.add(stmt);
            return true;
        }

        if (lexer.identifierEquals("LIST")) {
            StarrocksListStmt stmt = new StarrocksListStmt();

            lexer.nextToken();
            stmt.setObject(this.exprParser.expr());

            if (lexer.identifierEquals("ROLES")
                    && stmt.getObject() instanceof SQLIdentifierExpr && ((SQLIdentifierExpr) stmt.getObject()).nameEquals("TENANT")) {
                lexer.nextToken();
                stmt.setObject(new SQLIdentifierExpr("TENANT ROLES"));
            } else if (lexer.identifierEquals("OUTPUT")
                    && stmt.getObject() instanceof SQLIdentifierExpr && ((SQLIdentifierExpr) stmt.getObject()).nameEquals("TEMPORARY")) {
                lexer.nextToken();
                stmt.setObject(new SQLIdentifierExpr("TEMPORARY OUTPUT"));
            }

            statementList.add(stmt);

            return true;
        }

        if (lexer.token() == Token.DESC || lexer.identifierEquals("DESCRIBE")) {
            SQLStatement stmt = parseDescribe();
            statementList.add(stmt);
            return true;
        }

        if (lexer.identifierEquals("WHOAMI")) {
            lexer.nextToken();
            SQLWhoamiStatement stmt = new SQLWhoamiStatement();
            statementList.add(stmt);
            return true;
        }

        if (lexer.identifierEquals("COUNT")) {
            lexer.nextToken();
            StarrocksCountStatement stmt = new StarrocksCountStatement();
            stmt.setTable(this.exprParser.name());

            if (lexer.token() == Token.PARTITION) {
                lexer.nextToken();
                this.exprParser.parseAssignItem(stmt.getPartitions(), stmt);
            }
            statementList.add(stmt);
            return true;
        }

        if (lexer.identifierEquals("MSCK")) {
            SQLStatement stmt = parseMsck();
            statementList.add(stmt);
            return true;
        }

        if (lexer.identifierEquals("EXSTORE")) {
            lexer.nextToken();
            StarrocksExstoreStatement stmt = new StarrocksExstoreStatement();
            SQLExpr table = this.exprParser.expr();
            stmt.setTable(new SQLExprTableSource(table));
            accept(Token.PARTITION);
            this.exprParser.parseAssignItem(stmt.getPartitions(), stmt);
            statementList.add(stmt);
            return true;
        }

        if (lexer.identifierEquals("INSTALL")) {
            lexer.nextToken();
            acceptIdentifier("PACKAGE");
            StarrocksInstallPackageStatement stmt = new StarrocksInstallPackageStatement();
            stmt.setPackageName(
                    this.exprParser.name()
            );
            statementList.add(stmt);
            return true;
        }

        if (lexer.identifierEquals(FnvHash.Constants.KILL)) {
            SQLStatement stmt = parseKill();
            statementList.add(stmt);
            return true;
        }

        if (lexer.identifierEquals(FnvHash.Constants.LOAD)) {
            StarrocksLoadDataStatement stmt = parseLoad();

            statementList.add(stmt);
            return true;
        }

        if (lexer.identifierEquals(FnvHash.Constants.MERGE)) {
            SQLStatement stmt = parseMerge();
            statementList.add(stmt);
            return true;
        }

        if (lexer.identifierEquals(FnvHash.Constants.CLONE)) {
            SQLStatement stmt = parseClone();
            statementList.add(stmt);
            return true;
        }

        if (lexer.identifierEquals(FnvHash.Constants.UNLOAD)) {
            SQLStatement stmt = parseUnload();
            statementList.add(stmt);
            return true;
        }

        if (lexer.identifierEquals(FnvHash.Constants.BEGIN)) {
            SQLStatement stmt = parseBlock();
            statementList.add(stmt);
            return true;
        }

        if (lexer.identifierEquals(FnvHash.Constants.RESTORE)) {
            lexer.nextToken();
            accept(Token.TABLE);
            StarrocksRestoreStatement stmt = new StarrocksRestoreStatement();
            stmt.setTable(this.exprParser.name());

            if (lexer.token() == Token.LPAREN) {
                this.exprParser.parseAssignItem(stmt.getProperties(), stmt);
            }

            if (lexer.token() == Token.PARTITION) {
                lexer.nextToken();
                this.exprParser.parseAssignItem(stmt.getPartitions(), stmt);
            }

            if (lexer.token() == Token.TO) {
                lexer.nextToken();
                acceptIdentifier("LSN");
                stmt.setTo(
                        this.exprParser.expr()
                );
            }

            if (lexer.token() == Token.AS) {
                lexer.nextToken();
                stmt.setAlias(
                        this.alias()
                );
            }

            statementList.add(stmt);
            return true;
        }

        if (lexer.identifierEquals(FnvHash.Constants.UNDO)) {
            lexer.nextToken();
            accept(Token.TABLE);
            StarrocksUndoTableStatement stmt = new StarrocksUndoTableStatement();
            stmt.setTable(
                    new SQLExprTableSource(
                            this.exprParser.name()
                    )
            );

            if (lexer.token() == Token.PARTITION) {
                lexer.nextToken();
                this.exprParser.parseAssignItem(stmt.getPartitions(), stmt);
            }
            accept(Token.TO);
            stmt.setTo(
                    this.exprParser.expr()
            );
            statementList.add(stmt);
            return true;
        }

        if (lexer.token() == Token.FUNCTION) {
            StarrocksCreateFunctionStatement stmt = (StarrocksCreateFunctionStatement) parseStarrocksCreateFunction();
            stmt.setDeclare(true);
            statementList.add(stmt);
            return true;
        }

        if (lexer.token() == Token.VARIANT && lexer.stringVal().startsWith("@")) {
            Lexer.SavePoint mark = lexer.mark();
            String variant = lexer.stringVal();
            lexer.nextToken();

            if (lexer.token() == Token.COLONEQ) {
                lexer.nextToken();

                boolean cache = false;
                if (lexer.identifierEquals(FnvHash.Constants.CACHE)) {
                    lexer.nextToken();
                    accept(Token.ON);
                    cache = true;
                }

                Lexer.SavePoint lpMark = null;
                if (lexer.token() == Token.LPAREN) {
                    lpMark = lexer.mark();
                    lexer.nextToken();
                }

                switch (lexer.token()) {
                    case LITERAL_INT:
                    case LITERAL_FLOAT:
                    case LITERAL_CHARS:
                    case LITERAL_ALIAS:
                    case IDENTIFIER:
                    case CASE:
                    case CAST:
                    case IF:
                    case VARIANT:
                    case REPLACE:
                    case NEW:
                    case SUB:
                    case TRUE:
                    case FALSE: {
                        if (lpMark != null) {
                            lexer.reset(lpMark);
                        }

                        SQLExpr expr = this.exprParser.expr();
                        SQLExprStatement stmt = new SQLExprStatement(
                                new SQLAssignItem(new SQLIdentifierExpr(variant), expr)
                        );
                        statementList.add(stmt);
                        return true;
                    }
                    default:
                        if (lpMark != null) {
                            lexer.reset(lpMark);
                        }

                        boolean paren = lexer.token() == Token.LPAREN;
                        Lexer.SavePoint parenMark = lexer.mark();
                        SQLSelect select;
                        try {
                            select = new StarrocksSelectParser(this.exprParser)
                                    .select();
                        } catch (ParserException error) {
                            if (paren) {
                                lexer.reset(parenMark);
                                SQLExpr expr = this.exprParser.expr();
                                SQLExprStatement stmt = new SQLExprStatement(
                                        new SQLAssignItem(new SQLIdentifierExpr(variant), expr)
                                );
                                statementList.add(stmt);
                                return true;
                            }
                            throw error;
                        }
                        switch (lexer.token()) {
                            case GT:
                            case GTEQ:
                            case EQ:
                            case LT:
                            case LTEQ:
                                statementList.add(
                                        new SQLExprStatement(
                                                new SQLAssignItem(new SQLIdentifierExpr(variant),
                                                        this.exprParser.exprRest(new SQLQueryExpr(select))
                                                )
                                        )
                                );
                                return true;
                            default:
                                break;
                        }
                        SQLSelectStatement stmt = new SQLSelectStatement(select, dbType);

                        StarrocksQueryAliasStatement aliasQueryStatement = new StarrocksQueryAliasStatement(variant, stmt);
                        aliasQueryStatement.setCache(cache);
                        statementList.add(aliasQueryStatement);
                        return true;
                }
            }

            StarrocksDeclareVariableStatement stmt = new StarrocksDeclareVariableStatement();

            if (lexer.token() != Token.EQ && lexer.token() != Token.SEMI && lexer.token() != Token.EOF) {
                stmt.setDataType(
                        this.exprParser.parseDataType()
                );
            }

            if (lexer.token() == Token.EQ || lexer.token() == Token.COLONEQ) {
                lexer.nextToken();
                stmt.setInitValue(
                        this.exprParser.expr()
                );
            }

            if (lexer.token() == Token.SEMI) {
                lexer.nextToken();
            }
            statementList.add(stmt);
            return true;
        }

        if (lexer.token() == Token.IF) {
            SQLStatement stmt = parseIf();
            statementList.add(stmt);
            return true;
        }

        if (lexer.token() == Token.CODE) {
            Lexer.SavePoint mark = lexer.mark();
            lexer.nextToken();
            if (lexer.token() == Token.EOF || lexer.token() == Token.SEMI) {
                return true;
            }
            lexer.reset(mark);
        }

        return false;
    }

    public SQLStatement parseIf() {
        accept(Token.IF);
        SQLIfStatement ifStmt = new SQLIfStatement();
        ifStmt.setCondition(
                this.exprParser.expr()
        );

        if (lexer.identifierEquals("BEGIN")) {
            lexer.nextToken();
            parseStatementList(ifStmt.getStatements(), -1, ifStmt);
            accept(Token.END);
        } else {
            SQLStatement stmt = parseStatement();
            ifStmt.getStatements().add(stmt);
            stmt.setParent(ifStmt);
        }

        if (lexer.token() == Token.SEMI) {
            lexer.nextToken();
        }

        if (lexer.token() == Token.ELSE) {
            lexer.nextToken();

            SQLIfStatement.Else elseItem = new SQLIfStatement.Else();
            if (lexer.identifierEquals("BEGIN")) {
                lexer.nextToken();
                parseStatementList(elseItem.getStatements(), -1, ifStmt);
                accept(Token.END);
            } else {
                SQLStatement stmt = parseStatement();
                elseItem.getStatements().add(stmt);
                stmt.setParent(elseItem);
            }
            ifStmt.setElseItem(elseItem);
        }

        return ifStmt;
    }

    public SQLStatement parseKill() {
        acceptIdentifier("KILL");
        MySqlKillStatement stmt = new MySqlKillStatement();
        SQLExpr instanceId = this.exprParser.primary();
        stmt.setThreadId(instanceId);
        return stmt;
    }

    public SQLStatement parseUnload() {
        acceptIdentifier("UNLOAD");
        StarrocksUnloadStatement stmt = new StarrocksUnloadStatement();

        accept(Token.FROM);
        if (lexer.token() == Token.LPAREN || lexer.token() == Token.SELECT) {
            stmt.setFrom(
                    this.createSQLSelectParser().parseTableSource()
            );
        } else {
            stmt.setFrom(
                    this.exprParser.name()
            );
        }

        if (lexer.token() == Token.PARTITION) {
            lexer.nextToken();
            this.exprParser.parseAssignItem(stmt.getPartitions(), stmt);
        }

        accept(Token.INTO);

        if (lexer.identifierEquals("LOCATION")) {
            lexer.nextToken();
            stmt.setLocation(this.exprParser.primary());
        }

        if (lexer.identifierEquals("ROW")) {
            SQLExternalRecordFormat format = this.exprParser.parseRowFormat();
            stmt.setRowFormat(format);
        }

        for (; ; ) {
            if (lexer.identifierEquals(FnvHash.Constants.STORED)) {
                lexer.nextToken();
                if (lexer.token() == Token.BY) {
                    lexer.nextToken();
                } else {
                    accept(Token.AS);
                }
                stmt.setStoredAs(
                        this.exprParser.name());
                continue;
            }

            if (lexer.token() == Token.WITH) {
                lexer.nextToken();
                acceptIdentifier("SERDEPROPERTIES");
                this.exprParser.parseAssignItem(stmt.getSerdeProperties(), stmt);
                continue;
            }

            if (identifierEquals("PROPERTIES")) {
                lexer.nextToken();
                this.exprParser.parseAssignItem(stmt.getProperties(), stmt);
                continue;
            }

            break;
        }

        return stmt;
    }

    public SQLStatement parseClone() {
        acceptIdentifier("CLONE");
        accept(Token.TABLE);
        SQLCloneTableStatement stmt = new SQLCloneTableStatement();

        stmt.setFrom(
                this.exprParser.name());

        if (lexer.token() == Token.PARTITION) {
            lexer.nextToken();
            this.exprParser.parseAssignItem(stmt.getPartitions(), stmt);
        }

        accept(Token.TO);

        stmt.setTo(
                this.exprParser.name());

        if (lexer.token() == Token.IF) {
            lexer.nextToken();
            accept(Token.EXISTS);

            if (lexer.token() == OVERWRITE) {
                lexer.nextToken();
                stmt.setIfExistsOverwrite(true);
            } else {
                acceptIdentifier("IGNORE");
                stmt.setIfExistsIgnore(true);
            }
        }

        return stmt;
    }

    public SQLStatement parseBlock() {
        SQLBlockStatement block = new SQLBlockStatement();
        if (lexer.identifierEquals(FnvHash.Constants.BEGIN)) {
            lexer.nextToken();
        } else {
            accept(Token.BEGIN);
        }
        this.parseStatementList(block.getStatementList(), -1, block);
        accept(Token.END);
        return block;
    }

    protected StarrocksStatisticClause parseStaticClause() {
        if (lexer.identifierEquals("TABLE_COUNT")) {
            lexer.nextToken();
            return new StarrocksStatisticClause.TableCount();
        } else if (lexer.identifierEquals("NULL_VALUE")) {
            lexer.nextToken();
            StarrocksStatisticClause.NullValue null_value = new StarrocksStatisticClause.NullValue();
            null_value.setColumn(this.exprParser.name());
            return null_value;
        } else if (lexer.identifierEquals("DISTINCT_VALUE")) {
            lexer.nextToken();
            StarrocksStatisticClause.DistinctValue distinctValue = new StarrocksStatisticClause.DistinctValue();
            distinctValue.setColumn(this.exprParser.name());
            return distinctValue;
        } else if (lexer.identifierEquals("COLUMN_SUM")) {
            lexer.nextToken();
            StarrocksStatisticClause.ColumnSum column_sum = new StarrocksStatisticClause.ColumnSum();
            column_sum.setColumn(this.exprParser.name());
            return column_sum;
        } else if (lexer.identifierEquals("COLUMN_MAX")) {
            lexer.nextToken();
            StarrocksStatisticClause.ColumnMax column_max = new StarrocksStatisticClause.ColumnMax();
            column_max.setColumn(this.exprParser.name());
            return column_max;
        } else if (lexer.identifierEquals("COLUMN_MIN")) {
            lexer.nextToken();
            StarrocksStatisticClause.ColumnMin column_min = new StarrocksStatisticClause.ColumnMin();
            column_min.setColumn(this.exprParser.name());
            return column_min;
        } else if (lexer.identifierEquals("EXPRESSION_CONDITION")) {
            lexer.nextToken();
            StarrocksStatisticClause.ExpressionCondition expr_condition = new StarrocksStatisticClause.ExpressionCondition();
            expr_condition.setExpr(this.exprParser.expr());
            return expr_condition;
        } else {
            throw new ParserException("TODO " + lexer.info());
        }
    }


    protected StarrocksInsert parseStarrocksInsert() {
        StarrocksInsert insert = new StarrocksInsert();

        if (lexer.isKeepComments() && lexer.hasComment()) {
            insert.addBeforeComment(lexer.readAndResetComments());
        }

        SQLSelectParser selectParser = createSQLSelectParser();

        accept(Token.INSERT);

        if (lexer.token() == Token.INTO) {
            lexer.nextToken();
        } else {
            accept(Token.OVERWRITE);
            insert.setOverwrite(true);
        }

        if (lexer.token() == Token.TABLE) {
            lexer.nextToken();
        }
        insert.setTableSource(this.exprParser.name());

        if (lexer.token() == Token.PARTITION) {
            lexer.nextToken();
            accept(Token.LPAREN);
            for (; ; ) {
                SQLAssignItem ptExpr = new SQLAssignItem();
                ptExpr.setTarget(this.exprParser.name());
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                    SQLExpr ptValue = this.exprParser.expr();
                    ptExpr.setValue(ptValue);
                }
                insert.addPartition(ptExpr);
                if (lexer.token() != Token.COMMA) {
                    break;
                } else {
                    lexer.nextToken();
                }
            }
            accept(Token.RPAREN);
        }

        if (lexer.token() == LPAREN) {
            lexer.nextToken();
            this.exprParser.exprList(insert.getColumns(), insert);
            accept(RPAREN);
        }

        if (lexer.token() == Token.VALUES) {
            lexer.nextToken();

            for (; ; ) {
                if (lexer.token() == Token.LPAREN) {
                    lexer.nextToken();

                    SQLInsertStatement.ValuesClause values = new SQLInsertStatement.ValuesClause();
                    this.exprParser.exprList(values.getValues(), values);
                    insert.addValueCause(values);
                    accept(Token.RPAREN);
                }

                if (lexer.token() == Token.COMMA) {
                    lexer.nextToken();
                    continue;
                } else {
                    break;
                }
            }
        } else {
            SQLSelect query = selectParser.select();
            insert.setQuery(query);
        }

        return insert;
    }


    protected StarrocksInsertStatement parseStarrocksInsertStmt() {
        StarrocksInsertStatement insert = new StarrocksInsertStatement();
        insert.setDbType(dbType);

        if (lexer.isKeepComments() && lexer.hasComment()) {
            insert.addInsertBeforeComment(lexer.readAndResetComments());
        }

        SQLSelectParser selectParser = createSQLSelectParser();

        accept(Token.INSERT);

        if (lexer.token() == Token.INTO) {
            lexer.nextToken();
        } else {
            accept(Token.OVERWRITE);
            insert.setOverwrite(true);
        }

        if (lexer.token() == Token.TABLE) {
            lexer.nextToken();
        }
        insert.setTableSource(this.exprParser.name());

        boolean columnsParsed = false;

        if (lexer.token() == (Token.LPAREN)) {
            Lexer.SavePoint mark = lexer.mark();
            lexer.nextToken();
            if (lexer.token() == Token.SELECT) {
                lexer.reset(mark);
            } else {
                parseInsertColumns(insert);
                columnsParsed = true;
                accept(Token.RPAREN);
            }
        }

        if (lexer.token() == Token.PARTITION) {
            lexer.nextToken();
            accept(Token.LPAREN);
            for (; ; ) {
                SQLAssignItem ptExpr = new SQLAssignItem();
                ptExpr.setTarget(this.exprParser.name());
                if (lexer.token() == Token.EQ || lexer.token() == Token.EQEQ) {
                    lexer.nextTokenValue();
                    SQLExpr ptValue = this.exprParser.expr();
                    ptExpr.setValue(ptValue);
                }
                insert.addPartition(ptExpr);
                if (!(lexer.token() == (Token.COMMA))) {
                    break;
                } else {
                    lexer.nextToken();
                }
            }
            accept(Token.RPAREN);
        }

        if (!columnsParsed && lexer.token() == Token.LPAREN) {
            Lexer.SavePoint m1 = lexer.mark();

            lexer.nextToken();
            boolean select;
            if (lexer.token() == LPAREN) {
                Lexer.SavePoint m2 = lexer.mark();
                lexer.nextToken();
                select = lexer.token() == SELECT;
                lexer.reset(m2);
            } else {
                select = lexer.token() == SELECT;
            }
            if (!select) {
                parseInsertColumns(insert);
                accept(Token.RPAREN);
            } else {
                lexer.reset(m1);
            }
        }

        if (lexer.token() == Token.IF) {
            lexer.nextToken();
            accept(Token.NOT);
            accept(Token.EXISTS);
            insert.setIfNotExists(true);
        }

        if (lexer.token() == Token.VALUES) {
            lexer.nextToken();

            for (; ; ) {
                if (lexer.token() == Token.LPAREN) {
                    lexer.nextToken();

                    SQLInsertStatement.ValuesClause values = new SQLInsertStatement.ValuesClause();
                    this.exprParser.exprList(values.getValues(), values);
                    insert.addValueCause(values);
                    accept(Token.RPAREN);
                }

                if (lexer.token() == Token.COMMA) {
                    lexer.nextToken();
                    continue;
                } else {
                    break;
                }
            }
        } else {
            SQLSelect query = selectParser.select();
            insert.setQuery(query);
        }

        return insert;
    }


    public SQLStatement parseInsert() {
        if (lexer.token() == Token.FROM) {
            lexer.nextToken();

            StarrocksMultiInsertStatement stmt = new StarrocksMultiInsertStatement();

            if (lexer.token() == Token.IDENTIFIER || lexer.token() == Token.VARIANT) {
                Lexer.SavePoint mark = lexer.mark();
                SQLExpr tableName = this.exprParser.name();
                if (lexer.token() == Token.LPAREN) {
                    lexer.reset(mark);
                    tableName = this.exprParser.primary();
                }

                SQLTableSource from = new SQLExprTableSource(tableName);

                if (lexer.token() == Token.IDENTIFIER) {
                    String alias = alias();
                    from.setAlias(alias);
                }

                SQLSelectParser selectParser = createSQLSelectParser();
                from = selectParser.parseTableSourceRest(from);

                if (lexer.token() == Token.WHERE) {
                    lexer.nextToken();
                    SQLExpr where = this.exprParser.expr();
                    SQLSelectQueryBlock queryBlock = new SQLSelectQueryBlock();
                    queryBlock.addSelectItem(new SQLAllColumnExpr());
                    queryBlock.setFrom(from);
                    queryBlock.setWhere(where);

                    if (lexer.token() == Token.GROUP) {
                        selectParser.parseGroupBy(queryBlock);
                    }

                    stmt.setFrom(
                            new SQLSubqueryTableSource(queryBlock)
                    );
                } else {
                    stmt.setFrom(from);
                }
            } else {
                SQLCommentHint hint = null;
                if (lexer.token() == Token.HINT) {
                    hint = this.exprParser.parseHint();
                }
                accept(Token.LPAREN);

                boolean paren2 = lexer.token() == Token.LPAREN;

                SQLSelectParser selectParser = createSQLSelectParser();
                SQLSelect select = selectParser.select();

                SQLTableSource from = null;
                if (paren2 && lexer.token() != Token.RPAREN) {
                    String subQueryAs = null;
                    if (lexer.token() == Token.AS) {
                        lexer.nextToken();
                        subQueryAs = tableAlias(true);
                    } else {
                        subQueryAs = tableAlias(false);
                    }
                    SQLSubqueryTableSource subQuery = new SQLSubqueryTableSource(select, subQueryAs);
                    from = selectParser.parseTableSourceRest(subQuery);
                }

                accept(Token.RPAREN);

                String alias;

                if (lexer.token() == Token.INSERT) {
                    alias = null;
                } else if (lexer.token() == Token.SELECT) {
                    // skip
                    alias = null;
                } else {
                    if (lexer.token() == Token.AS) {
                        lexer.nextToken();
                    }
                    alias = lexer.stringVal();
                    accept(Token.IDENTIFIER);
                }

                if (from == null) {
                    from = new SQLSubqueryTableSource(select, alias);
                } else {
                    if (alias != null) {
                        from.setAlias(alias);
                    }
                }

                SQLTableSource tableSource = selectParser.parseTableSourceRest(from);

                if (hint != null) {
                    if (tableSource instanceof SQLJoinTableSource) {
                        ((SQLJoinTableSource) tableSource).setHint(hint);
                    }
                }

                stmt.setFrom(tableSource);
            }

            if (lexer.token() == Token.SELECT) {
                SQLSelectParser selectParser = createSQLSelectParser();
                SQLSelect query = selectParser.select();

                StarrocksInsert insert = new StarrocksInsert();
                insert.setQuery(query);
                stmt.addItem(insert);
                return stmt;
            }

            for (; ; ) {
                StarrocksInsert insert = parseStarrocksInsert();
                stmt.addItem(insert);

                if (lexer.token() != Token.INSERT) {
                    break;
                }
            }

            return stmt;
        }

        return parseStarrocksInsertStmt();
    }

    public SQLSelectParser createSQLSelectParser() {
        return new StarrocksSelectParser(this.exprParser, selectListCache);
    }

    public SQLStatement parseShow() {
        accept(Token.SHOW);

        if (lexer.identifierEquals(FnvHash.Constants.PARTITIONS)) {
            lexer.nextToken();

            SQLShowPartitionsStmt stmt = new SQLShowPartitionsStmt();

            SQLExpr expr = this.exprParser.expr();
            stmt.setTableSource(new SQLExprTableSource(expr));

            if (lexer.token() == Token.PARTITION) {
                lexer.nextToken();
                accept(Token.LPAREN);
                parseAssignItems(stmt.getPartition(), stmt, false);
                accept(Token.RPAREN);
            }

            if (lexer.token() == Token.WHERE) {
                lexer.nextToken();
                stmt.setWhere(
                        this.exprParser.expr()
                );
            }

            return stmt;
        }

        if (lexer.identifierEquals(FnvHash.Constants.STATISTIC)) {
            lexer.nextToken();

            SQLShowStatisticStmt stmt = new SQLShowStatisticStmt();

            SQLExpr expr = this.exprParser.expr();
            stmt.setTableSource(new SQLExprTableSource(expr));

            if (lexer.token() == Token.PARTITION) {
                lexer.nextToken();

                accept(Token.LPAREN);
                parseAssignItems(stmt.getPartitions(), stmt, false);
                accept(Token.RPAREN);
            }

            if (identifierEquals("COLUMNS")) {
                lexer.nextToken();

                if (lexer.token() != Token.SEMI) {
                    accept(Token.LPAREN);
                    this.exprParser.names(stmt.getColumns(), stmt);
                    accept(Token.RPAREN);
                }
            }

            return stmt;
        }

        if (lexer.identifierEquals(FnvHash.Constants.STATISTIC_LIST)) {
            lexer.nextToken();

            SQLShowStatisticListStmt stmt = new SQLShowStatisticListStmt();

            SQLExpr expr = this.exprParser.expr();
            stmt.setTableSource(new SQLExprTableSource(expr));

            return stmt;
        }

        if (lexer.identifierEquals(FnvHash.Constants.PACKAGES)) {
            lexer.nextToken();

            SQLShowPackagesStatement stmt = new SQLShowPackagesStatement();
            return stmt;
        }

        if (lexer.identifierEquals(FnvHash.Constants.TABLES)) {
            lexer.nextToken();

            SQLShowTablesStatement stmt = new SQLShowTablesStatement();

            if (lexer.token() == Token.FROM || lexer.token() == Token.IN) {
                lexer.nextToken();
                stmt.setDatabase(this.exprParser.name());
            } else if (lexer.token() == IDENTIFIER) {
                SQLName database = exprParser.name();
                stmt.setDatabase(database);
            }

            if (lexer.token() == Token.LIKE) {
                lexer.nextToken();
                stmt.setLike(this.exprParser.expr());
            } else if (lexer.token() == Token.LITERAL_CHARS || lexer.token() == Token.LITERAL_ALIAS) {
                stmt.setLike(this.exprParser.expr());
            }

            return stmt;
        }

        if (lexer.identifierEquals(FnvHash.Constants.LABEL)) {
            lexer.nextToken();
            acceptIdentifier("GRANTS");
            StarrocksShowGrantsStmt stmt = new StarrocksShowGrantsStmt();
            stmt.setLabel(true);

            if (lexer.token() == Token.ON) {
                lexer.nextToken();
                accept(Token.TABLE);
                stmt.setObjectType(this.exprParser.expr());
            }

            if (lexer.token() == Token.FOR) {
                lexer.nextToken();
                accept(Token.USER);
                stmt.setUser(this.exprParser.expr());
            }

            return stmt;
        }

        if (lexer.identifierEquals(FnvHash.Constants.GRANTS)) {
            lexer.nextToken();
            StarrocksShowGrantsStmt stmt = new StarrocksShowGrantsStmt();

            if (lexer.token() == Token.FOR) {
                lexer.nextToken();
                if (lexer.token() == Token.USER) {
                    lexer.nextToken();
                }
                stmt.setUser(this.exprParser.expr());
            }

            if (lexer.token() == Token.ON) {
                lexer.nextToken();
                acceptIdentifier("type");
                stmt.setObjectType(this.exprParser.expr());
            }

            return stmt;
        }

        if (lexer.identifierEquals(FnvHash.Constants.USERS)) {
            lexer.nextToken();
            SQLShowUsersStatement stmt = new SQLShowUsersStatement();
            return stmt;
        }

        if (lexer.identifierEquals("RECYCLEBIN")) {
            lexer.nextToken();
            SQLShowRecylebinStatement stmt = new SQLShowRecylebinStatement();
            return stmt;
        }

        if (lexer.identifierEquals("VARIABLES")) {
            lexer.nextToken();
            return parseShowVariants();
        }

        if (lexer.token() == Token.CREATE) {
            return parseShowCreateTable();
        }

        if (lexer.identifierEquals(FnvHash.Constants.FUNCTIONS)) {
            lexer.nextToken();

            SQLShowFunctionsStatement stmt = new SQLShowFunctionsStatement();
            if (lexer.token() == Token.LIKE) {
                lexer.nextToken();
                stmt.setLike(
                        this.exprParser.expr()
                );
            } else if (lexer.token() == Token.LITERAL_CHARS || lexer.token() == IDENTIFIER) {
                stmt.setLike(
                        this.exprParser.expr()
                );
            }

            return stmt;
        }

        if (lexer.identifierEquals(FnvHash.Constants.ROLE)) {
            lexer.nextToken();

            SQLShowRoleStatement stmt = new SQLShowRoleStatement();

            if (lexer.token() == Token.GRANT) {
                lexer.nextToken();
                stmt.setGrant(
                        this.exprParser.name()
                );
            }
            return stmt;
        }

        if (lexer.identifierEquals("ACL")) {
            lexer.nextToken();

            SQLShowACLStatement stmt = new SQLShowACLStatement();

            if (lexer.token() == Token.FOR) {
                lexer.nextToken();
                stmt.setTable(
                        new SQLExprTableSource(
                                this.exprParser.name()
                        )
                );
            }
            return stmt;
        }

        if (lexer.identifierEquals(FnvHash.Constants.ROLES)) {
            lexer.nextToken();

            SQLShowRolesStatement stmt = new SQLShowRolesStatement();
            return stmt;
        }

        if (lexer.identifierEquals("HISTORY")) {
            lexer.nextToken();
            SQLShowHistoryStatement stmt = new SQLShowHistoryStatement();

            if (lexer.token() == Token.FOR) {
                lexer.nextToken();
                if (lexer.identifierEquals(FnvHash.Constants.TABLES)) {
                    lexer.nextToken();
                    stmt.setTables(true);
                } else if (lexer.token() == Token.TABLE) {
                    lexer.nextToken();
                    stmt.setTable(
                            new SQLExprTableSource(
                                    this.exprParser.name()
                            )
                    );
                }
            }

            if (lexer.token() == Token.LPAREN) {
                this.exprParser.parseAssignItem(stmt.getProperties(), stmt);
            }

            if (lexer.token() == Token.PARTITION) {
                lexer.nextToken();
                this.exprParser.parseAssignItem(stmt.getPartitions(), stmt);
            }

            return stmt;
        }

        if (lexer.identifierEquals("CHANGELOGS")) {
            lexer.nextToken();
            StarrocksShowChangelogsStatement stmt = new StarrocksShowChangelogsStatement();

            if (lexer.token() == Token.FOR) {
                lexer.nextToken();
                if (lexer.identifierEquals(FnvHash.Constants.TABLES)) {
                    lexer.nextToken();
                    stmt.setTables(true);
                } else if (lexer.token() == Token.TABLE) {
                    lexer.nextToken();
                    stmt.setTable(
                            new SQLExprTableSource(
                                    this.exprParser.name()
                            )
                    );
                } else if (lexer.token() == IDENTIFIER) {
                    stmt.setTable(
                            new SQLExprTableSource(
                                    this.exprParser.name()
                            )
                    );
                }
            }

            if (lexer.token() == Token.LPAREN) {
                this.exprParser.parseAssignItem(stmt.getProperties(), stmt);
            }

            if (lexer.token() == Token.PARTITION) {
                lexer.nextToken();
                this.exprParser.parseAssignItem(stmt.getPartitions(), stmt);
            }

            if (lexer.token() == Token.LITERAL_INT) {
                stmt.setId(
                        this.exprParser.primary()
                );
            }

            return stmt;
        }

        throw new ParserException("TODO " + lexer.info());
    }

    public SQLStatement parseSet() {
        List<String> comments = null;
        if (lexer.isKeepComments() && lexer.hasComment()) {
            comments = lexer.readAndResetComments();
        }

        boolean setProject = false;
        if (identifierEquals("SETPROJECT")) {
            lexer.nextToken();
            setProject = true;
        } else {
            accept(Token.SET);
        }

        if (lexer.token() == Token.SET && dbType == DbType.starrocks) {
            lexer.nextToken();
        }

        if (lexer.identifierEquals("PROJECT")) {
            lexer.nextToken();
            setProject = true;
        }

        if (setProject) {
            SQLSetStatement stmt = new SQLSetStatement();
            stmt.setOption(SQLSetStatement.Option.PROJECT);
            SQLName target = this.exprParser.name();
            accept(Token.EQ);
            SQLExpr value = this.exprParser.expr();
            stmt.set(target, value);
            return stmt;
        } else if (lexer.identifierEquals("LABEL")) {
            StarrocksSetLabelStatement stmt = new StarrocksSetLabelStatement();

            if (comments != null) {
                stmt.addBeforeComment(comments);
            }

            lexer.nextToken();

            stmt.setLabel(lexer.stringVal());
            lexer.nextToken();
            accept(Token.TO);
            if (lexer.token() == Token.USER) {
                lexer.nextToken();

                SQLName name = this.exprParser.name();
                stmt.setUser(name);
                return stmt;
            }
            accept(Token.TABLE);
            SQLExpr expr = this.exprParser.name();
            stmt.setTable(new SQLExprTableSource(expr));

            if (lexer.token() == Token.LPAREN) {
                lexer.nextToken();
                this.exprParser.names(stmt.getColumns(), stmt);
                accept(Token.RPAREN);
            }

            return stmt;
        } else {
            SQLSetStatement stmt = new SQLSetStatement(dbType);
            stmt.putAttribute("parser.set", Boolean.TRUE);

            if (comments != null) {
                stmt.addBeforeComment(comments);
            }

            parseAssignItems(stmt.getItems(), stmt);

            return stmt;
        }
    }

    public StarrocksGrantStmt parseGrant() {
        accept(Token.GRANT);
        StarrocksGrantStmt stmt = new StarrocksGrantStmt();

        if (lexer.identifierEquals("LABEL")) {
            stmt.setLabel(true);
            lexer.nextToken();
            stmt.setLabel(this.exprParser.expr());
        } else {
            if (lexer.identifierEquals("SUPER")) {
                stmt.setSuper(true);
                lexer.nextToken();
            }

            parsePrivileages(stmt.getPrivileges(), stmt);
        }

        if (lexer.token() == Token.ON) {
            lexer.nextToken();

            if (lexer.identifierEquals("PROJECT")) {
                lexer.nextToken();
                stmt.setResourceType(SQLObjectType.PROJECT);
            } else if (lexer.identifierEquals("PACKAGE")) {
                lexer.nextToken();
                stmt.setResourceType(SQLObjectType.PACKAGE);
            } else if (lexer.token() == Token.FUNCTION) {
                lexer.nextToken();
                stmt.setResourceType(SQLObjectType.FUNCTION);
            } else if (lexer.token() == Token.TABLE) {
                lexer.nextToken();
                stmt.setResourceType(SQLObjectType.TABLE);
                if (lexer.token() == Token.LPAREN) {
                    lexer.nextToken();
                    this.exprParser.names(stmt.getColumns(), stmt);
                    accept(Token.RPAREN);
                }
            } else if (lexer.identifierEquals("RESOURCE")) {
                lexer.nextToken();
                stmt.setResourceType(SQLObjectType.RESOURCE);
            } else if (lexer.identifierEquals("INSTANCE")) {
                lexer.nextToken();
                stmt.setResourceType(SQLObjectType.INSTANCE);
            } else if (lexer.identifierEquals("JOB")) {
                lexer.nextToken();
                stmt.setResourceType(SQLObjectType.JOB);
            } else if (lexer.identifierEquals("VOLUME")) {
                lexer.nextToken();
                stmt.setResourceType(SQLObjectType.VOLUME);
            } else if (lexer.identifierEquals("OfflineModel")) {
                lexer.nextToken();
                stmt.setResourceType(SQLObjectType.OfflineModel);
            } else if (lexer.identifierEquals("XFLOW")) {
                lexer.nextToken();
                stmt.setResourceType(SQLObjectType.XFLOW);
            }

            stmt.setResource(this.exprParser.expr());
        }

        if (lexer.token() == Token.TO) {
            lexer.nextToken();
            if (lexer.token() == Token.USER) {
                lexer.nextToken();
                stmt.setSubjectType(SQLObjectType.USER);
            } else if (lexer.identifierEquals("ROLE")) {
                lexer.nextToken();
                stmt.setSubjectType(SQLObjectType.ROLE);
            }
            stmt.getUsers().add(this.exprParser.expr());
        }

        if (lexer.token() == Token.WITH) {
            lexer.nextToken();
            acceptIdentifier("EXP");
            stmt.setExpire(this.exprParser.expr());
        }

        return stmt;
    }

    protected void parsePrivileages(List<SQLPrivilegeItem> privileges, SQLObject parent) {
        for (; ; ) {
            String privilege = null;
            if (lexer.token() == Token.ALL) {
                lexer.nextToken();
                privilege = "ALL";
            } else if (lexer.token() == Token.SELECT) {
                privilege = "SELECT";
                lexer.nextToken();
            } else if (lexer.token() == Token.UPDATE) {
                privilege = "UPDATE";
                lexer.nextToken();
            } else if (lexer.token() == Token.DELETE) {
                privilege = "DELETE";
                lexer.nextToken();
            } else if (lexer.token() == Token.INSERT) {
                privilege = "INSERT";
                lexer.nextToken();
            } else if (lexer.token() == Token.DROP) {
                lexer.nextToken();
                privilege = "DROP";
            } else if (lexer.token() == Token.ALTER) {
                lexer.nextToken();
                privilege = "ALTER";
            } else if (lexer.identifierEquals("DESCRIBE")) {
                privilege = "DESCRIBE";
                lexer.nextToken();
            } else if (lexer.identifierEquals("READ")) {
                privilege = "READ";
                lexer.nextToken();
            } else if (lexer.identifierEquals("WRITE")) {
                privilege = "WRITE";
                lexer.nextToken();
            } else if (lexer.identifierEquals("EXECUTE")) {
                lexer.nextToken();
                privilege = "EXECUTE";
            } else if (lexer.identifierEquals("LIST")) {
                lexer.nextToken();
                privilege = "LIST";
            } else if (lexer.identifierEquals("CreateTable")) {
                lexer.nextToken();
                privilege = "CreateTable";
            } else if (lexer.identifierEquals("CreateInstance")) {
                lexer.nextToken();
                privilege = "CreateInstance";
            } else if (lexer.identifierEquals("CreateFunction")) {
                lexer.nextToken();
                privilege = "CreateFunction";
            } else if (lexer.identifierEquals("CreateResource")) {
                lexer.nextToken();
                privilege = "CreateResource";
            } else if (lexer.identifierEquals("CreateJob")) {
                lexer.nextToken();
                privilege = "CreateJob";
            } else if (lexer.identifierEquals("CreateVolume")) {
                lexer.nextToken();
                privilege = "CreateVolume";
            } else if (lexer.identifierEquals("CreateOfflineModel")) {
                lexer.nextToken();
                privilege = "CreateOfflineModel";
            } else if (lexer.identifierEquals("CreateXflow")) {
                lexer.nextToken();
                privilege = "CreateXflow";
            }

            SQLExpr expr = null;
            if (privilege != null) {
                expr = new SQLIdentifierExpr(privilege);
            } else {
                expr = this.exprParser.expr();
            }

            SQLPrivilegeItem privilegeItem = new SQLPrivilegeItem();
            privilegeItem.setAction(expr);

            if (lexer.token() == Token.LPAREN) {
                lexer.nextToken();
                for (; ; ) {
                    privilegeItem.getColumns().add(this.exprParser.name());

                    if (lexer.token() == Token.COMMA) {
                        lexer.nextToken();
                        continue;
                    }
                    break;
                }
                accept(Token.RPAREN);
            }

            expr.setParent(parent);
            privileges.add(privilegeItem);

            if (lexer.token() == Token.COMMA) {
                lexer.nextToken();
                continue;
            }
            break;
        }
    }

    public SQLCreateFunctionStatement parseCreateFunction() {
        return parseStarrocksCreateFunction();
    }

    protected StarrocksLoadDataStatement parseLoad() {
        acceptIdentifier("LOAD");

        StarrocksLoadDataStatement stmt = new StarrocksLoadDataStatement();

        if (lexer.token() == OVERWRITE) {
            stmt.setOverwrite(true);
            lexer.nextToken();
        } else if (lexer.token() == Token.INTO) {
            lexer.nextToken();
        }

        accept(Token.TABLE);

        stmt.setInto(
                this.exprParser.expr());

        if (lexer.token() == Token.PARTITION) {
            lexer.nextToken();
            accept(Token.LPAREN);
            this.exprParser.exprList(stmt.getPartition(), stmt);
            accept(Token.RPAREN);
        }

        if (lexer.identifierEquals(FnvHash.Constants.LOCAL)) {
            lexer.nextToken();
            stmt.setLocal(true);
        }

        accept(Token.FROM);

        acceptIdentifier("LOCATION");

        SQLExpr inpath = this.exprParser.expr();
        stmt.setInpath(inpath);

        if (lexer.identifierEquals("STORED")) {
            lexer.nextToken();

            if (lexer.token() == Token.BY) {
                lexer.nextToken();
                stmt.setStoredBy(this.exprParser.expr());
            } else {
                accept(Token.AS);
                stmt.setStoredAs(this.exprParser.expr());
            }
        }

        if (lexer.identifierEquals("ROW")) {
            lexer.nextToken();

            acceptIdentifier("FORMAT");
            acceptIdentifier("SERDE");
            stmt.setRowFormat(this.exprParser.expr());
        }

        if (lexer.token() == Token.WITH) {
            lexer.nextToken();
            acceptIdentifier("SERDEPROPERTIES");

            accept(Token.LPAREN);

            for (; ; ) {
                String name = lexer.stringVal();
                lexer.nextToken();
                accept(Token.EQ);
                SQLExpr value = this.exprParser.primary();
                stmt.getSerdeProperties().put(name, value);
                if (lexer.token() == Token.COMMA) {
                    lexer.nextToken();
                    continue;
                }
                break;
            }

            accept(Token.RPAREN);
        }

        if (lexer.identifierEquals("STORED")) {
            lexer.nextToken();

            accept(Token.AS);
            stmt.setStoredAs(this.exprParser.expr());
        }

        if (lexer.identifierEquals(FnvHash.Constants.USING)) {
            lexer.nextToken();
            stmt.setUsing(
                    this.exprParser.expr()
            );
        }

        return stmt;
    }
}
