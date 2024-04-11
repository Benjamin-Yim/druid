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
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.druid.sql.ast.statement.SQLExternalRecordFormat;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.dialect.oracle.ast.OracleSegmentAttributes;
import com.alibaba.druid.sql.dialect.oracle.ast.clause.OracleLobStorageClause;
import com.alibaba.druid.sql.dialect.oracle.ast.clause.OracleStorageClause;
import com.alibaba.druid.sql.dialect.starrocks.ast.StarrocksNewExpr;
import com.alibaba.druid.sql.dialect.starrocks.ast.StarrocksPartitionValue;
import com.alibaba.druid.sql.dialect.starrocks.ast.StarrocksTransformExpr;
import com.alibaba.druid.sql.dialect.starrocks.ast.StarrocksUDTFSQLSelectItem;
import com.alibaba.druid.sql.parser.*;
import com.alibaba.druid.util.FnvHash;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class StarrocksExprParser extends SQLExprParser {
    public static final String[] AGGREGATE_FUNCTIONS;

    public static final long[] AGGREGATE_FUNCTIONS_CODES;

    static {
        String[] strings = {
                "AVG",
                "COUNT",
                "LAG",
                "LEAD",
                "MAX",
                "MIN",
                "STDDEV",
                "SUM",
                "ROW_NUMBER",
                "WM_CONCAT",
                "STRAGG",
                "COLLECT_LIST",
                "COLLECT_SET"//
        };
        AGGREGATE_FUNCTIONS_CODES = FnvHash.fnv1a_64_lower(strings, true);
        AGGREGATE_FUNCTIONS = new String[AGGREGATE_FUNCTIONS_CODES.length];
        for (String str : strings) {
            long hash = FnvHash.fnv1a_64_lower(str);
            int index = Arrays.binarySearch(AGGREGATE_FUNCTIONS_CODES, hash);
            AGGREGATE_FUNCTIONS[index] = str;
        }
    }

    public StarrocksExprParser(Lexer lexer) {
        super(lexer, DbType.starrocks);

        this.aggregateFunctions = AGGREGATE_FUNCTIONS;
        this.aggregateFunctionHashCodes = AGGREGATE_FUNCTIONS_CODES;
    }

    public StarrocksExprParser(String sql, SQLParserFeature... features) {
        this(new StarrocksLexer(sql, features));
        this.lexer.nextToken();
    }

    public StarrocksExprParser(String sql, boolean skipComments, boolean keepComments) {
        this(new StarrocksLexer(sql, skipComments, keepComments));
        this.lexer.nextToken();
    }

    protected SQLExpr parseAliasExpr(String alias) {
        String chars = alias.substring(1, alias.length() - 1);
        return new SQLCharExpr(chars);
    }

    static final long GSONBUILDER = FnvHash.fnv1a_64_lower("GSONBUILDER");

    @Override
    public SQLSelectItem parseSelectItem() {
        SQLExpr expr;
        if (lexer.token() == Token.IDENTIFIER) {
            String stringVal = lexer.stringVal();
            long hash_lower = lexer.hashLCase();

            lexer.nextTokenComma();

            if (FnvHash.Constants.DATETIME == hash_lower
                    && lexer.stringVal().charAt(0) != '`'
                    && (lexer.token() == Token.LITERAL_CHARS
                    || lexer.token() == Token.LITERAL_ALIAS)
            ) {
                String literal = lexer.stringVal();
                lexer.nextToken();

                SQLDateTimeExpr ts = new SQLDateTimeExpr(literal);
                expr = ts;
            } else if (FnvHash.Constants.DATE == hash_lower
                    && lexer.stringVal().charAt(0) != '`'
                    && (lexer.token() == Token.LITERAL_CHARS
                    || lexer.token() == Token.LITERAL_ALIAS)
            ) {
                String literal = lexer.stringVal();
                lexer.nextToken();

                SQLDateExpr d = new SQLDateExpr(literal);
                expr = d;
            } else if (FnvHash.Constants.TIMESTAMP == hash_lower
                    && lexer.stringVal().charAt(0) != '`'
                    && (lexer.token() == Token.LITERAL_CHARS
                    || lexer.token() == Token.LITERAL_ALIAS)
            ) {
                String literal = lexer.stringVal();
                lexer.nextToken();

                SQLTimestampExpr ts = new SQLTimestampExpr(literal);
                expr = ts;
            } else {
                expr = new SQLIdentifierExpr(stringVal);
                if (lexer.token() != Token.COMMA) {
                    expr = this.primaryRest(expr);
                    expr = this.exprRest(expr);
                }
            }
        } else {
            expr = expr();
        }

        String alias = null;
        if (lexer.token() == Token.AS) {
            lexer.nextToken();

            if (lexer.token() == Token.LPAREN) {
                lexer.nextToken();

                StarrocksUDTFSQLSelectItem selectItem = new StarrocksUDTFSQLSelectItem();

                selectItem.setExpr(expr);

                for (; ; ) {
                    alias = lexer.stringVal();
                    lexer.nextToken();

                    selectItem.getAliasList().add(alias);

                    if (lexer.token() == Token.COMMA) {
                        lexer.nextToken();
                        continue;
                    }
                    break;
                }

                accept(Token.RPAREN);

                return selectItem;
            } else {
                alias = alias();
            }
        } else {
            alias = as();
        }

        SQLSelectItem item = new SQLSelectItem(expr, alias);

        if (lexer.hasComment() && lexer.isKeepComments()) {
            item.addAfterComment(lexer.readAndResetComments());
        }

        return item;
    }

    public SQLExpr primaryRest(SQLExpr expr) {
        if (lexer.token() == Token.COLON) {
            lexer.nextToken();
            if (lexer.token() == Token.LITERAL_INT && expr instanceof SQLPropertyExpr) {
                SQLPropertyExpr propertyExpr = (SQLPropertyExpr) expr;
                Number integerValue = lexer.integerValue();
                lexer.nextToken();
                propertyExpr.setName(propertyExpr.getName() + ':' + integerValue.intValue());
                return propertyExpr;
            }
            expr = dotRest(expr);
            return expr;
        }

        if (lexer.token() == Token.LBRACKET) {
            SQLArrayExpr array = new SQLArrayExpr();
            array.setExpr(expr);
            lexer.nextToken();
            this.exprList(array.getValues(), array);
            accept(Token.RBRACKET);
            return primaryRest(array);
        } else if ((lexer.token() == Token.LITERAL_CHARS || lexer.token() == Token.LITERAL_ALIAS) && expr instanceof SQLCharExpr) {
            SQLCharExpr charExpr = new SQLCharExpr(lexer.stringVal());
            lexer.nextTokenValue();
            SQLMethodInvokeExpr concat = new SQLMethodInvokeExpr("concat", null, expr, charExpr);

            while (lexer.token() == Token.LITERAL_CHARS || lexer.token() == Token.LITERAL_ALIAS) {
                charExpr = new SQLCharExpr(lexer.stringVal());
                lexer.nextToken();
                concat.addArgument(charExpr);
            }

            expr = concat;
        }

        if (lexer.token() == Token.LPAREN
                && expr instanceof SQLIdentifierExpr
                && ((SQLIdentifierExpr) expr).nameHashCode64() == FnvHash.Constants.TRANSFORM) {
            StarrocksTransformExpr transformExpr = new StarrocksTransformExpr();
            lexer.nextToken();
            this.exprList(transformExpr.getInputColumns(), transformExpr);
            accept(Token.RPAREN);

            if (lexer.identifierEquals(FnvHash.Constants.ROW)) {
                SQLExternalRecordFormat recordFormat = this.parseRowFormat();
                transformExpr.setInputRowFormat(recordFormat);
            }

            if (lexer.token() == Token.USING || lexer.identifierEquals(FnvHash.Constants.USING)) {
                lexer.nextToken();
                transformExpr.setUsing(this.expr());
            }

            if (lexer.identifierEquals(FnvHash.Constants.RESOURCES)) {
                lexer.nextToken();
                this.exprList(transformExpr.getResources(), transformExpr);
            }

            if (lexer.token() == Token.AS) {
                lexer.nextToken();
                List<SQLColumnDefinition> outputColumns = transformExpr.getOutputColumns();

                if (lexer.token() == Token.LPAREN) {
                    lexer.nextToken();
                    for (; ; ) {
                        SQLColumnDefinition column = this.parseColumn();
                        outputColumns.add(column);
                        if (lexer.token() == Token.COMMA) {
                            lexer.nextToken();
                            continue;
                        }
                        break;
                    }
                    accept(Token.RPAREN);
                } else {
                    SQLColumnDefinition column = new SQLColumnDefinition();
                    column.setName(this.name());
                    outputColumns.add(column);
                }
            }

            if (lexer.identifierEquals(FnvHash.Constants.ROW)) {
                SQLExternalRecordFormat recordFormat = this.parseRowFormat();
                transformExpr.setOutputRowFormat(recordFormat);
            }

            return transformExpr;
        }

        if (expr instanceof SQLIdentifierExpr
                && ((SQLIdentifierExpr) expr).nameHashCode64() == FnvHash.Constants.NEW) {
            SQLIdentifierExpr ident = (SQLIdentifierExpr) expr;

            StarrocksNewExpr newExpr = new StarrocksNewExpr();
            if (lexer.token() == Token.IDENTIFIER) { //.GSON
                Lexer.SavePoint mark = lexer.mark();

                String methodName = lexer.stringVal();
                lexer.nextToken();
                switch (lexer.token()) {
                    case ON:
                    case WHERE:
                    case GROUP:
                    case ORDER:
                    case INNER:
                    case JOIN:
                    case FULL:
                    case OUTER:
                    case LEFT:
                    case RIGHT:
                    case LATERAL:
                    case FROM:
                    case COMMA:
                    case RPAREN:
                        return ident;
                    default:
                        break;
                }

                while (lexer.token() == Token.DOT) {
                    lexer.nextToken();
                    methodName += '.' + lexer.stringVal();
                    lexer.nextToken();
                }

                newExpr.setMethodName(methodName);

                if (lexer.token() == Token.LT) {
                    lexer.nextToken();
                    for (; ; ) {
                        if (lexer.token() == Token.GT) {
                            break;
                        }
                        SQLDataType paramType = this.parseDataType(false);
                        paramType.setParent(newExpr);
                        newExpr.getTypeParameters().add(paramType);
                        if (lexer.token() == Token.COMMA) {
                            lexer.nextToken();
                            continue;
                        }
                        break;
                    }
                    accept(Token.GT);
                }

                if (lexer.token() == Token.LBRACKET) {
                    lexer.nextToken();
                    this.exprList(newExpr.getArguments(), newExpr);
                    accept(Token.RBRACKET);
                    if (lexer.token() == Token.LBRACKET) {
                        lexer.nextToken();
                        accept(Token.RBRACKET);
                    }
                    newExpr.setArray(true);

                    if (lexer.token() == Token.LBRACE) {
                        lexer.nextToken();
                        for (; ; ) {
                            if (lexer.token() == Token.RPAREN) {
                                break;
                            }

                            SQLExpr item = this.expr();
                            newExpr.getInitValues().add(item);
                            item.setParent(newExpr);

                            if (lexer.token() == Token.COMMA) {
                                lexer.nextToken();
                                continue;
                            }
                            break;
                        }
                        accept(Token.RBRACE);
                    }
                    if (lexer.token() == Token.LBRACKET) {
                        expr = primaryRest(newExpr);
                    } else {
                        expr = newExpr;
                    }
                } else {
                    accept(Token.LPAREN);
                    this.exprList(newExpr.getArguments(), newExpr);
                    accept(Token.RPAREN);
                    expr = newExpr;
                }
            } else if (lexer.identifierEquals("java") || lexer.identifierEquals("com")) {
                SQLName name = this.name();
                String strName = ident.getName() + ' ' + name.toString();
                if (lexer.token() == Token.LT) {
                    lexer.nextToken();
                    for (int i = 0; lexer.token() != Token.GT; i++) {
                        if (i != 0) {
                            strName += ", ";
                        }
                        SQLName arg = this.name();
                        strName += arg.toString();
                    }
                    lexer.nextToken();
                }
                ident.setName(strName);
            }
        }

        if (expr == null) {
            return null;
        }

        return super.primaryRest(expr);
    }

    public SQLExpr relationalRest(SQLExpr expr) {
        if (lexer.identifierEquals("REGEXP")) {
            lexer.nextToken();
            SQLExpr rightExp = bitOr();

            rightExp = relationalRest(rightExp);

            return new SQLBinaryOpExpr(expr, SQLBinaryOperator.RegExp, rightExp, dbType);
        }

        return super.relationalRest(expr);
    }

    @Override
    public StarrocksSelectParser createSelectParser() {
        return new StarrocksSelectParser(this);
    }

    protected SQLPartitionByRange partitionByRange() {
        acceptIdentifier("RANGE");
        accept(Token.LPAREN);
        SQLPartitionByRange clause = new SQLPartitionByRange();
        for (; ; ) {
            SQLName column = this.name();
            clause.addColumn(column);

            if (lexer.token() == Token.COMMA) {
                lexer.nextToken();
                continue;
            }

            break;
        }
        accept(Token.RPAREN);

        parsePartitionByRest(clause);

        return clause;
    }

    protected void parsePartitionByRest(SQLPartitionBy clause) {
        accept(Token.LPAREN);

        for (; ; ) {
            SQLPartition partition;
            if (lexer.token() == Token.START) {
                partition = this.parseMultiRangePartition();
            } else {
                partition = this.parsePartition();
            }

            clause.addPartition(partition);

            if (lexer.token() == Token.COMMA) {
                lexer.nextToken();
                continue;
            }

            break;
        }

        accept(Token.RPAREN);
    }

    protected SQLSubPartitionBy subPartitionBy() {
        lexer.nextToken();
        accept(Token.BY);

        if (lexer.identifierEquals(FnvHash.Constants.HASH)) {
            lexer.nextToken();
            accept(Token.LPAREN);

            SQLSubPartitionByHash byHash = new SQLSubPartitionByHash();
            SQLExpr expr = this.expr();
            byHash.setExpr(expr);
            accept(Token.RPAREN);

            return byHash;
        } else if (lexer.identifierEquals(FnvHash.Constants.LIST)) {
            lexer.nextToken();
            accept(Token.LPAREN);

            SQLSubPartitionByList byList = new SQLSubPartitionByList();
            SQLName column = this.name();
            byList.setColumn(column);
            accept(Token.RPAREN);

            if (lexer.identifierEquals(FnvHash.Constants.SUBPARTITION)) {
                lexer.nextToken();
                acceptIdentifier("TEMPLATE");
                accept(Token.LPAREN);

                for (; ; ) {
                    SQLSubPartition subPartition = this.parseSubPartition();
                    subPartition.setParent(byList);
                    byList.getSubPartitionTemplate().add(subPartition);

                    if (lexer.token() == Token.COMMA) {
                        lexer.nextToken();
                        continue;
                    }
                    break;
                }
                accept(Token.RPAREN);
            }

            if (lexer.identifierEquals(FnvHash.Constants.SUBPARTITIONS)) {
                lexer.nextToken();
                Number intValue = lexer.integerValue();
                SQLNumberExpr numExpr = new SQLNumberExpr(intValue);
                byList.setSubPartitionsCount(numExpr);
                lexer.nextToken();
            }

            return byList;
        }

        throw new ParserException("TODO : " + lexer.info());
    }

    protected SQLSubPartition parseSubPartition() {
        acceptIdentifier("SUBPARTITION");

        SQLSubPartition subPartition = new SQLSubPartition();
        SQLName name = this.name();
        subPartition.setName(name);

        SQLPartitionValue values = this.parsePartitionValues();
        if (values != null) {
            subPartition.setValues(values);
        }

        if (lexer.token() == Token.TABLESPACE) {
            lexer.nextToken();
            subPartition.setTableSpace(this.name());
        }

        return subPartition;
    }

    protected void partitionClauseRest(SQLPartitionBy clause) {
        if (lexer.identifierEquals(FnvHash.Constants.PARTITIONS)) {
            lexer.nextToken();

            SQLIntegerExpr countExpr = this.integerExpr();
            clause.setPartitionsCount(countExpr);
        }

        if (lexer.token() == Token.STORE) {
            lexer.nextToken();
            accept(Token.IN);
            accept(Token.LPAREN);
            this.names(clause.getStoreIn(), clause);
            accept(Token.RPAREN);
        }
    }


    public SQLPartitionValue parsePartitionValues() {
        if (lexer.token() != Token.VALUES) {
            return null;
        }
        lexer.nextToken();

        SQLPartitionValue values = null;

        if (lexer.token() == Token.IN) {
            lexer.nextToken();
            values = new SQLPartitionValue(SQLPartitionValue.Operator.In);

            accept(Token.LPAREN);
            this.exprList(values.getItems(), values);
            accept(Token.RPAREN);
        } else if (lexer.identifierEquals(FnvHash.Constants.LESS)) {
            lexer.nextToken();
            acceptIdentifier("THAN");

            values = new SQLPartitionValue(SQLPartitionValue.Operator.LessThan);

            if (lexer.identifierEquals(FnvHash.Constants.MAXVALUE)) {
                SQLIdentifierExpr maxValue = new SQLIdentifierExpr(lexer.stringVal());
                lexer.nextToken();
                maxValue.setParent(values);
                values.addItem(maxValue);
            } else {
                accept(Token.LPAREN);
                this.exprList(values.getItems(), values);
                accept(Token.RPAREN);
            }
        } else if (lexer.token() == Token.LPAREN) {
            values = new SQLPartitionValue(SQLPartitionValue.Operator.List);
            lexer.nextToken();
            this.exprList(values.getItems(), values);
            accept(Token.RPAREN);
        } else if (lexer.token() == Token.LBRACKET) {
            values = new SQLPartitionValue(SQLPartitionValue.Operator.List);
            lexer.nextToken();
            this.exprList(values.getItems(), values);
            accept(Token.RPAREN);
        }
        return values;
    }


    protected SQLPartition parsePartition() {
        accept(Token.PARTITION);
        SQLPartition partition = new SQLPartition();
        partition.setName(this.name());

        SQLPartitionValue values = this.parsePartitionValues();
        if (values != null) {
            this.parseSegmentAttributes(values);
        }

        if (values != null) {
            partition.setValues(values);
        }

        if (lexer.token() == Token.LPAREN) {
            lexer.nextToken();

            for (; ; ) {
                SQLSubPartition subPartition = parseSubPartition();
                this.parseSegmentAttributes(subPartition);

                partition.addSubPartition(subPartition);

                if (lexer.token() == Token.COMMA) {
                    lexer.nextToken();
                    continue;
                }

                break;
            }

            accept(Token.RPAREN);
        } else if (lexer.identifierEquals(FnvHash.Constants.SUBPARTITIONS)) {
            lexer.nextToken();
            SQLExpr subPartitionsCount = this.primary();
            partition.setSubPartitionsCount(subPartitionsCount);
        }

        for (; ; ) {
            parseSegmentAttributes(partition);

            if (lexer.token() == Token.LOB) {
                OracleLobStorageClause lobStorage = this.parseLobStorage();
                partition.setLobStorage(lobStorage);
                continue;
            }

            if (lexer.token() == Token.SEGMENT || lexer.identifierEquals("SEGMENT")) {
                lexer.nextToken();
                accept(Token.CREATION);
                if (lexer.token() == Token.IMMEDIATE) {
                    lexer.nextToken();
                    partition.setSegmentCreationImmediate(true);
                } else if (lexer.token() == Token.DEFERRED) {
                    lexer.nextToken();
                    partition.setSegmentCreationDeferred(true);
                }
                continue;
            }
            break;
        }

        if (lexer.token() == Token.LPAREN) {
            lexer.nextToken();

            for (; ; ) {
                SQLSubPartition subPartition = parseSubPartition();
                this.parseSegmentAttributes(subPartition);

                partition.addSubPartition(subPartition);

                if (lexer.token() == Token.COMMA) {
                    lexer.nextToken();
                    continue;
                }

                break;
            }

            accept(Token.RPAREN);
        }

        return partition;
    }

    protected SQLPartition parseMultiRangePartition() {
        SQLPartition partition = new SQLPartition();

        SQLPartitionValue values = this.parseMultiRangePartitionValues();
        if (values != null) {
            this.parseSegmentAttributes(values);
        }

        if (values != null) {
            partition.setValues(values);
        }

        return partition;
    }

    public SQLPartitionValue parseMultiRangePartitionValues() {
        lexer.nextToken();
        SQLPartitionValue values = null;
        if (lexer.token() == Token.LPAREN) {
            lexer.nextToken();

            values = new StarrocksPartitionValue(SQLPartitionValue.Operator.Between_and);
            SQLIdentifierExpr minValue = new SQLIdentifierExpr(lexer.stringVal());
            lexer.nextToken();
            minValue.setParent(values);
            values.addItem(minValue);
            accept(Token.RPAREN);

            accept(Token.END);
            lexer.nextToken();
            SQLIdentifierExpr maxValue = new SQLIdentifierExpr(lexer.stringVal());
            lexer.nextToken();
            maxValue.setParent(values);
            values.addItem(maxValue);

            accept(Token.RPAREN);
        }

        accept(Token.EVERY);

        if (lexer.token() == Token.LPAREN) {
            lexer.nextToken();
            List<String> unitString = new ArrayList<>();
            for (; ; ) {
                if (lexer.token() == Token.RPAREN) {
                    break;
                }
                if (lexer.token() == Token.LITERAL_INT) {
                    unitString.add(String.valueOf(lexer.integerValue()));
                } else {
                    unitString.add(lexer.stringVal());
                }

                lexer.nextToken();
            }
            SQLIdentifierExpr unit = new SQLIdentifierExpr(StringUtils.join(unitString, " "));
            unit.setParent(values);
            values.addItem(unit);
        }
        lexer.nextToken();
        return values;
    }

    public OracleLobStorageClause parseLobStorage() {
        lexer.nextToken();

        OracleLobStorageClause clause = new OracleLobStorageClause();

        accept(Token.LPAREN);
        this.names(clause.getItems());
        accept(Token.RPAREN);

        accept(Token.STORE);
        accept(Token.AS);

        if (lexer.identifierEquals("SECUREFILE")) {
            lexer.nextToken();
            clause.setSecureFile(true);
        }

        if (lexer.identifierEquals("BASICFILE")) {
            lexer.nextToken();
            clause.setBasicFile(true);
        }

        if (lexer.token() == Token.IDENTIFIER || lexer.token() == Token.LITERAL_ALIAS) {
            SQLName segmentName = this.name();
            clause.setSegementName(segmentName);
        }

        if (lexer.token() == Token.LPAREN) {
            lexer.nextToken();

            for (; ; ) {
                this.parseSegmentAttributes(clause);

                if (lexer.token() == Token.ENABLE) {
                    lexer.nextToken();
                    accept(Token.STORAGE);
                    accept(Token.IN);
                    accept(Token.ROW);
                    clause.setEnable(true);
                    continue;
                } else if (lexer.token() == Token.DISABLE) {
                    lexer.nextToken();
                    accept(Token.STORAGE);
                    accept(Token.IN);
                    accept(Token.ROW);
                    clause.setEnable(false);
                    continue;
                }

                if (lexer.token() == Token.CHUNK) {
                    lexer.nextToken();
                    clause.setChunk(this.primary());
                    continue;
                }

                if (lexer.token() == Token.NOCACHE) {
                    lexer.nextToken();
                    clause.setCache(false);
                    if (lexer.token() == Token.LOGGING) {
                        lexer.nextToken();
                        clause.setLogging(true);
                    }
                    continue;
                }

                if (lexer.token() == Token.CACHE) {
                    lexer.nextToken();
                    clause.setCache(true);
                    continue;
                }

                if (lexer.token() == Token.KEEP_DUPLICATES) {
                    lexer.nextToken();
                    clause.setKeepDuplicate(true);
                    continue;
                }

                if (lexer.identifierEquals("PCTVERSION")) {
                    lexer.nextToken();
                    clause.setPctversion(this.expr());
                    continue;
                }

                if (lexer.identifierEquals("RETENTION")) {
                    lexer.nextToken();
                    clause.setRetention(true);
                    continue;
                }

                if (lexer.token() == Token.STORAGE) {
                    OracleStorageClause storageClause = this.parseStorage();
                    clause.setStorageClause(storageClause);
                    continue;
                }

                break;
            }

            accept(Token.RPAREN);
        }

        return clause;
    }

    public void parseSegmentAttributes(OracleSegmentAttributes attributes) {
        for (; ; ) {
            if (lexer.token() == Token.TABLESPACE) {
                lexer.nextToken();
                attributes.setTablespace(this.name());
                continue;
            } else if (lexer.token() == Token.NOCOMPRESS || lexer.identifierEquals("NOCOMPRESS")) {
                lexer.nextToken();
                attributes.setCompress(Boolean.FALSE);
                continue;
            } else if (lexer.identifierEquals(FnvHash.Constants.COMPRESS)) {
                lexer.nextToken();
                attributes.setCompress(Boolean.TRUE);

                if (lexer.token() == Token.LITERAL_INT) {
                    int compressLevel = this.parseIntValue();
                    attributes.setCompressLevel(compressLevel);
                } else if (lexer.identifierEquals("BASIC")) {
                    lexer.nextToken();
                    // TODO COMPRESS BASIC
                } else if (lexer.token() == Token.FOR) {
                    lexer.nextToken();
                    if (lexer.identifierEquals("OLTP")) {
                        lexer.nextToken();
                        attributes.setCompressForOltp(true);
                    } else {
                        throw new ParserException("TODO : " + lexer.info());
                    }
                }
                continue;
            } else if (lexer.identifierEquals("NOCOMPRESS")) {
                lexer.nextToken();
                attributes.setCompress(Boolean.FALSE);
                continue;
            } else if (lexer.token() == Token.LOGGING || lexer.identifierEquals("LOGGING")) {
                lexer.nextToken();
                attributes.setLogging(Boolean.TRUE);
                continue;
            } else if (lexer.identifierEquals("NOLOGGING")) {
                lexer.nextToken();
                attributes.setLogging(Boolean.FALSE);
                continue;
            } else if (lexer.token() == Token.INITRANS) {
                lexer.nextToken();
                attributes.setInitrans(this.parseIntValue());
                continue;
            } else if (lexer.token() == Token.MAXTRANS) {
                lexer.nextToken();
                attributes.setMaxtrans(this.parseIntValue());
            } else if (lexer.token() == Token.PCTINCREASE) {
                lexer.nextToken();
                attributes.setPctincrease(this.parseIntValue());
                continue;
            } else if (lexer.token() == Token.PCTFREE) {
                lexer.nextToken();
                attributes.setPctfree(this.parseIntValue());
                continue;
            } else if (lexer.token() == Token.STORAGE || lexer.identifierEquals("STORAGE")) {
                OracleStorageClause storage = this.parseStorage();
                attributes.setStorage(storage);
                continue;
            } else if (lexer.identifierEquals(FnvHash.Constants.PCTUSED)) {
                lexer.nextToken();
                attributes.setPctused(this.parseIntValue());
                continue;
            } else if (lexer.identifierEquals(FnvHash.Constants.USAGE)) {
                lexer.nextToken();
                acceptIdentifier("QUEUE");
                // TODO USAGE QUEUE
                continue;
            } else if (lexer.identifierEquals(FnvHash.Constants.OPAQUE)) {
                parseOpaque();
                // TODO OPAQUE TYPE
                continue;
            } else {
                break;
            }
        }
    }

    public SQLObject parseOpaque() {
        acceptIdentifier("OPAQUE");
        acceptIdentifier("TYPE");

        SQLExpr expr = this.primary();

        OracleLobStorageClause clause = new OracleLobStorageClause();

        accept(Token.STORE);
        accept(Token.AS);

        if (lexer.identifierEquals("SECUREFILE")) {
            lexer.nextToken();
            clause.setSecureFile(true);
        }

        if (lexer.identifierEquals("BASICFILE")) {
            lexer.nextToken();
            clause.setBasicFile(true);
        }

        accept(Token.LOB);

        if (lexer.token() == Token.LPAREN) {
            lexer.nextToken();

            for (; ; ) {
                this.parseSegmentAttributes(clause);

                if (lexer.token() == Token.ENABLE) {
                    lexer.nextToken();
                    accept(Token.STORAGE);
                    accept(Token.IN);
                    accept(Token.ROW);
                    clause.setEnable(true);
                    continue;
                } else if (lexer.token() == Token.DISABLE) {
                    lexer.nextToken();
                    accept(Token.STORAGE);
                    accept(Token.IN);
                    accept(Token.ROW);
                    clause.setEnable(false);
                    continue;
                }

                if (lexer.token() == Token.CHUNK) {
                    lexer.nextToken();
                    clause.setChunk(this.primary());
                    continue;
                }

                if (lexer.token() == Token.NOCACHE) {
                    lexer.nextToken();
                    clause.setCache(false);
                    if (lexer.token() == Token.LOGGING) {
                        lexer.nextToken();
                        clause.setLogging(true);
                    }
                    continue;
                }

                if (lexer.token() == Token.CACHE) {
                    lexer.nextToken();
                    clause.setCache(true);
                    continue;
                }

                if (lexer.token() == Token.KEEP_DUPLICATES) {
                    lexer.nextToken();
                    clause.setKeepDuplicate(true);
                    continue;
                }

                if (lexer.identifierEquals("PCTVERSION")) {
                    lexer.nextToken();
                    clause.setPctversion(this.expr());
                    continue;
                }

                if (lexer.identifierEquals("RETENTION")) {
                    lexer.nextToken();
                    clause.setRetention(true);
                    continue;
                }

                if (lexer.token() == Token.STORAGE) {
                    OracleStorageClause storageClause = this.parseStorage();
                    clause.setStorageClause(storageClause);
                    continue;
                }

                break;
            }

            accept(Token.RPAREN);
        }
        return clause;
    }

    public OracleStorageClause parseStorage() {
        lexer.nextToken();
        accept(Token.LPAREN);

        OracleStorageClause storage = new OracleStorageClause();
        for (; ; ) {
            if (lexer.identifierEquals("INITIAL")) {
                lexer.nextToken();
                storage.setInitial(this.expr());
                continue;
            } else if (lexer.token() == Token.NEXT) {
                lexer.nextToken();
                storage.setNext(this.expr());
                continue;
            } else if (lexer.token() == Token.MINEXTENTS) {
                lexer.nextToken();
                storage.setMinExtents(this.expr());
                continue;
            } else if (lexer.token() == Token.MAXEXTENTS) {
                lexer.nextToken();
                storage.setMaxExtents(this.expr());
                continue;
            } else if (lexer.token() == Token.MAXSIZE) {
                lexer.nextToken();
                storage.setMaxSize(this.expr());
                continue;
            } else if (lexer.token() == Token.PCTINCREASE) {
                lexer.nextToken();
                storage.setPctIncrease(this.expr());
                continue;
            } else if (lexer.identifierEquals("FREELISTS")) {
                lexer.nextToken();
                storage.setFreeLists(this.expr());
                continue;
            } else if (lexer.identifierEquals("FREELIST")) {
                lexer.nextToken();
                acceptIdentifier("GROUPS");
                storage.setFreeListGroups(this.expr());
                continue;
            } else if (lexer.identifierEquals("BUFFER_POOL")) {
                lexer.nextToken();
                storage.setBufferPool(this.expr());
                continue;
            } else if (lexer.identifierEquals("OBJNO")) {
                lexer.nextToken();
                storage.setObjno(this.expr());
                continue;
            } else if (lexer.token() == Token.FLASH_CACHE) {
                lexer.nextToken();
                OracleStorageClause.FlashCacheType flashCacheType;
                if (lexer.identifierEquals("KEEP")) {
                    flashCacheType = OracleStorageClause.FlashCacheType.KEEP;
                    lexer.nextToken();
                } else if (lexer.token() == Token.NONE) {
                    flashCacheType = OracleStorageClause.FlashCacheType.NONE;
                    lexer.nextToken();
                } else {
                    accept(Token.DEFAULT);
                    flashCacheType = OracleStorageClause.FlashCacheType.DEFAULT;
                }
                storage.setFlashCache(flashCacheType);
                continue;
            } else if (lexer.token() == Token.CELL_FLASH_CACHE) {
                lexer.nextToken();
                OracleStorageClause.FlashCacheType flashCacheType;
                if (lexer.identifierEquals("KEEP")) {
                    flashCacheType = OracleStorageClause.FlashCacheType.KEEP;
                    lexer.nextToken();
                } else if (lexer.token() == Token.NONE) {
                    flashCacheType = OracleStorageClause.FlashCacheType.NONE;
                    lexer.nextToken();
                } else {
                    accept(Token.DEFAULT);
                    flashCacheType = OracleStorageClause.FlashCacheType.DEFAULT;
                }
                storage.setCellFlashCache(flashCacheType);
                continue;
            }

            break;
        }
        accept(Token.RPAREN);
        return storage;
    }

}
