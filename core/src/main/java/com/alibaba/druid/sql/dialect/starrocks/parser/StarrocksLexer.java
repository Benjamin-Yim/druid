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
import com.alibaba.druid.sql.parser.*;

import java.util.HashMap;
import java.util.Map;

import static com.alibaba.druid.sql.parser.CharTypes.*;
import static com.alibaba.druid.sql.parser.LayoutCharacters.EOI;

public class StarrocksLexer extends Lexer {
    public static final Keywords DEFAULT_STARROCKS_KEYWORDS;

    static {
        Map<String, Token> map = new HashMap<String, Token>();

        map.putAll(Keywords.DEFAULT_KEYWORDS.getKeywords());


        map.put("SHOW", Token.SHOW);
        map.put("PARTITION", Token.PARTITION);
        map.put("PARTITIONED", Token.PARTITIONED);
        map.put("OVERWRITE", Token.OVERWRITE);
        map.put("OVER", Token.OVER);
        map.put("LIMIT", Token.LIMIT);
        map.put("IF", Token.IF);
        map.put("DISTRIBUTE", Token.DISTRIBUTE);
        map.put("TRUE", Token.TRUE);
        map.put("FALSE", Token.FALSE);
        map.put("RLIKE", Token.RLIKE);
        map.put("DIV", Token.DIV);
        map.put("DUPLICATE", Token.DUPLICATE);
        map.put("START", Token.START);
        map.put("END", Token.END);
        map.put("EVERY", Token.EVERY);
        map.put("AGGREGATE", Token.AGGREGATE);
        map.put("UNIQUE", Token.UNIQUE);
        map.put("PRIMARY", Token.PRIMARY);
        map.put("DISTRIBUTED", Token.DISTRIBUTED);
        map.put("PROPERTIES", Token.PROPERTIES);
//        map.put("SUM", Token.SUM);
//        map.put("AVG", Token.AVG);
//        map.put("COUNT", Token.COUNT);
//        map.put("MAX", Token.MAX);
//        map.put("MIN", Token.MIN);
        map.put("ANY_VALUE", Token.ANY_VALUE);
        map.put("APPROX_COUNT_DISTINCT", Token.APPROX_COUNT_DISTINCT);
        map.put("APPROX_TOP_K", Token.APPROX_TOP_K);
        map.put("BITMAP", Token.BITMAP);
        map.put("CORR", Token.CORR);
        map.put("COUNT_IF", Token.COUNT_IF);
        map.put("COVAR_POP", Token.COVAR_POP);
        map.put("COVAR_SAMP", Token.COVAR_SAMP);
        map.put("GROUP_CONCAT", Token.GROUP_CONCAT);
        map.put("GROUPING", Token.GROUPING);
        map.put("GROUPING_ID", Token.GROUPING_ID);
        map.put("HLL_RAW_AGG", Token.HLL_RAW_AGG);
        map.put("HLL_UNION", Token.HLL_UNION);
        map.put("HLL_UNION_AGG", Token.HLL_UNION_AGG);
        map.put("MAX_BY", Token.MAX_BY);
        map.put("MIN_BY", Token.MIN_BY);
        map.put("MULTI_DISTINCT_COUNT", Token.MULTI_DISTINCT_COUNT);
        map.put("MULTI_DISTINCT_SUM", Token.MULTI_DISTINCT_SUM);
        map.put("PERCENTILE_APPROX", Token.PERCENTILE_APPROX);
        map.put("PERCENTILE_CONT", Token.PERCENTILE_CONT);
        map.put("PERCENTILE_DISC", Token.PERCENTILE_DISC);
        map.put("RETENTION", Token.RETENTION);
        map.put("STD", Token.STD);
        map.put("STDDEV", Token.STDDEV);
        map.put("STDDEV_POP", Token.STDDEV_POP);
        map.put("STDDEV_SAMP", Token.STDDEV_SAMP);
        map.put("VAR_SAMP", Token.VAR_SAMP);
        map.put("VARIANCE_SAMP", Token.VARIANCE_SAMP);
        map.put("VARIANCE", Token.VARIANCE);
        map.put("VAR_POP", Token.VAR_POP);
        map.put("VARIANCE_POP", Token.VARIANCE_POP);
        map.put("WINDOW_FUNNEL", Token.WINDOW_FUNNEL);
        map.put("PERCENTILE_UNION", Token.PERCENTILE_UNION);
        map.put("INDEX", Token.INDEX);
        map.put("USING", Token.USING);
        map.put("BITMAP_UNION", Token.BITMAP_UNION);
        DEFAULT_STARROCKS_KEYWORDS = new Keywords(map);
    }

    public StarrocksLexer(String input, SQLParserFeature... features) {
        super(input);

        init();

        dbType = DbType.starrocks;
        super.keywords = DEFAULT_STARROCKS_KEYWORDS;
        this.skipComment = true;
        this.keepComments = false;

        for (SQLParserFeature feature : features) {
            config(feature, true);
        }
    }

    public StarrocksLexer(String input, boolean skipComment, boolean keepComments) {
        super(input, skipComment);

        init();

        dbType = DbType.starrocks;
        this.skipComment = skipComment;
        this.keepComments = keepComments;
        super.keywords = DEFAULT_STARROCKS_KEYWORDS;
    }

    public StarrocksLexer(String input, CommentHandler commentHandler) {
        super(input, commentHandler);

        init();

        dbType = DbType.starrocks;
        super.keywords = DEFAULT_STARROCKS_KEYWORDS;
    }

    private void init() {
        if (ch == '】' || ch == ' ' || ch == '，' || ch == '：' || ch == '、' || ch == '\u200C' || ch == '；') {
            ch = charAt(++pos);
        }

        if (ch == '上' && charAt(pos + 1) == '传') {
            pos += 2;
            ch = charAt(pos);

            while (isWhitespace(ch)) {
                ch = charAt(++pos);
            }
        }
    }

    protected final void scanStarrocksComment() {
        if (ch != '/' && ch != '-') {
            throw new IllegalStateException();
        }

        Token lastToken = this.token;

        mark = pos;
        bufPos = 0;
        scanChar();

        if (ch == ' ') {
            mark = pos;
            bufPos = 0;
            scanChar();
        }

        // /*+ */
        if (ch == '*') {
            scanChar();
            bufPos++;

            while (ch == ' ') {
                scanChar();
                bufPos++;
            }

            boolean isHint = false;
            int startHintSp = bufPos + 1;
            if (ch == '+') {
                isHint = true;
                scanChar();
                bufPos++;
            }

            for (; ; ) {
                if (ch == '*') {
                    if (charAt(pos + 1) == '/') {
                        bufPos += 2;
                        scanChar();
                        scanChar();
                        break;
                    } else if (isWhitespace(charAt(pos + 1))) {
                        int i = 2;
                        for (; i < 1024 * 1024; ++i) {
                            if (!isWhitespace(charAt(pos + i))) {
                                break;
                            }
                        }
                        if (charAt(pos + i) == '/') {
                            bufPos += 2;
                            pos += (i + 1);
                            ch = charAt(pos);
                            break;
                        }
                    }
                }

                scanChar();
                if (ch == EOI) {
                    break;
                }
                bufPos++;
            }

            if (isHint) {
                stringVal = subString(mark + startHintSp, (bufPos - startHintSp) - 1);
                token = Token.HINT;
            } else {
                stringVal = subString(mark, bufPos + 1);
                token = Token.MULTI_LINE_COMMENT;
                commentCount++;
                if (keepComments) {
                    addComment(stringVal);
                }
            }

            if (commentHandler != null && commentHandler.handle(lastToken, stringVal)) {
                return;
            }

            if (token != Token.HINT && !isAllowComment()) {
                throw new NotAllowCommentException();
            }

            return;
        }

        if (!isAllowComment()) {
            throw new NotAllowCommentException();
        }

        if (ch == '/' || ch == '-') {
            scanChar();
            bufPos++;

            for (; ; ) {
                if (ch == '\r') {
                    if (charAt(pos + 1) == '\n') {
                        line++;
                        bufPos += 2;
                        scanChar();
                        break;
                    }
                    bufPos++;
                    break;
                } else if (ch == EOI) {
                    if (pos >= text.length()) {
                        break;
                    }
                }

                if (ch == '\n') {
                    line++;
                    scanChar();
                    bufPos++;
                    break;
                }

                scanChar();
                bufPos++;
            }

            stringVal = subString(mark, ch != EOI ? bufPos : bufPos + 1);
            token = Token.LINE_COMMENT;
            commentCount++;
            if (keepComments) {
                addComment(stringVal);
            }
            endOfComment = isEOF();

            if (commentHandler != null && commentHandler.handle(lastToken, stringVal)) {
                return;
            }

            return;
        }
    }

    public void scanComment() {
        scanStarrocksComment();
    }

    public void scanIdentifier() {
        hashLCase = 0;
        hash = 0;

        final char first = ch;

        if (first == '`') {
            mark = pos;
            bufPos = 1;
            char ch;
            for (; ; ) {
                ch = charAt(++pos);

                if (ch == '`') {
                    bufPos++;
                    ch = charAt(++pos);
                    if (ch == '`') {
                        ch = charAt(++pos);
                        continue;
                    }
                    break;
                } else if (ch == EOI) {
                    throw new ParserException("illegal identifier. " + info());
                }

                bufPos++;
                continue;
            }

            this.ch = charAt(pos);

            stringVal = subString(mark, bufPos);
            token = Token.IDENTIFIER;

            return;
        }

        final boolean firstFlag = isFirstIdentifierChar(first)
                || ch == 'å'
                || ch == 'ß'
                || ch == 'ç';
        if (!firstFlag) {
            throw new ParserException("illegal identifier. " + info());
        }

        mark = pos;
        bufPos = 1;
        char ch;
        for (; ; ) {
            ch = charAt(++pos);

            if (ch != 'ó'
                    && ch != 'å'
                    && ch != 'é'
                    && ch != 'í'
                    && ch != 'ß'
                    && ch != 'ü'
                    && !isIdentifierChar(ch)) {
                if (ch == '{' && charAt(pos - 1) == '$') {
                    int endIndex = this.text.indexOf('}', pos);
                    if (endIndex != -1) {
                        bufPos += (endIndex - pos + 1);
                        pos = endIndex;
                        continue;
                    }
                }

                if (ch == '-'
                        && bufPos > 7
                        && text.regionMatches(false, mark, "ALIYUN$", 0, 7)) {
                    continue;
                }
                break;
            }

            if (ch == '；') {
                break;
            }

            bufPos++;
            continue;
        }

        this.ch = charAt(pos);

        if (ch == '@') { // for user identifier
            bufPos++;
            for (; ; ) {
                ch = charAt(++pos);

                if (ch != '-' && ch != '.' && !isIdentifierChar(ch)) {
                    break;
                }

                bufPos++;
                continue;
            }
        }
        this.ch = charAt(pos);

        // bufPos
        {
            final int LEN = "USING#CODE".length();
            if (bufPos == LEN && text.regionMatches(mark, "USING#CODE", 0, LEN)) {
                bufPos = "USING".length();
                pos -= 5;
                this.ch = charAt(pos);
            }
        }

        stringVal = addSymbol();
        Token tok = keywords.getKeyword(stringVal);
        if (tok != null) {
            token = tok;
        } else {
            token = Token.IDENTIFIER;
        }
    }

    public void scanVariable() {
        if (ch == ':') {
            token = Token.COLON;
            ch = charAt(++pos);
            return;
        }

        if (ch == '#'
                && (charAt(pos + 1) == 'C' || charAt(pos + 1) == 'c')
                && (charAt(pos + 2) == 'O' || charAt(pos + 2) == 'o')
                && (charAt(pos + 3) == 'D' || charAt(pos + 3) == 'd')
                && (charAt(pos + 4) == 'E' || charAt(pos + 4) == 'e')
        ) {
            int p1 = text.indexOf("#END CODE", pos + 1);
            int p2 = text.indexOf("#end code", pos + 1);
            if (p1 == -1) {
                p1 = p2;
            } else if (p1 > p2 && p2 != -1) {
                p1 = p2;
            }

            if (p1 != -1) {
                int end = p1 + "#END CODE".length();
                stringVal = text.substring(pos, end);
                token = Token.CODE;
                pos = end;
                ch = charAt(pos);
                return;
            }
        }

        super.scanVariable();
    }

    protected void scanVariable_at() {
        scanVariable();
    }

    protected final void scanString() {
        scanString2();
    }
}
