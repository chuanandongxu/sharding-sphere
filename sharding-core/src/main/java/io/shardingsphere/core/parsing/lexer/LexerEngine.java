/*
 * Copyright 2016-2018 shardingsphere.io.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * </p>
 */

package io.shardingsphere.core.parsing.lexer;

import com.google.common.collect.Sets;
import io.shardingsphere.core.constant.DatabaseType;
import io.shardingsphere.core.parsing.lexer.dialect.h2.H2Lexer;
import io.shardingsphere.core.parsing.lexer.dialect.mysql.MySQLLexer;
import io.shardingsphere.core.parsing.lexer.dialect.oracle.OracleLexer;
import io.shardingsphere.core.parsing.lexer.dialect.postgresql.PostgreSQLLexer;
import io.shardingsphere.core.parsing.lexer.dialect.sqlserver.SQLServerLexer;
import io.shardingsphere.core.parsing.lexer.token.Assist;
import io.shardingsphere.core.parsing.lexer.token.Symbol;
import io.shardingsphere.core.parsing.lexer.token.Token;
import io.shardingsphere.core.parsing.lexer.token.TokenType;
import io.shardingsphere.core.parsing.parser.exception.SQLParsingException;
import io.shardingsphere.core.parsing.parser.exception.SQLParsingUnsupportedException;
import io.shardingsphere.core.parsing.parser.sql.SQLStatement;
import lombok.RequiredArgsConstructor;

import java.util.Set;

/**
 * Lexical analysis engine.
 *
 * @author zhangliang
 */
@RequiredArgsConstructor
public final class LexerEngine {
    /**
     * 词法解析器
     */
    private final Lexer lexer;
    
    /**
     * Get input string.
     * 
     * @return inputted string
     */
    public String getInput() {
        return lexer.getInput();
    }
    
    /**
     * Analyse next token.
     */
    public void nextToken() {
        lexer.nextToken();
    }
    
    /**
     * Is end or not.
     *
     * @return current token is end token or not.
     */
    public boolean isEnd() {
        return Assist.END == lexer.getCurrentToken().getType();
    }
    
    /**
     * Get current token.
     * 
     * @return current token
     */
    public Token getCurrentToken() {
        return lexer.getCurrentToken();
    }
    
    /**
     * skip all tokens that inside parentheses.
     * 跳过小括号内所有的词法标记.
     * @param sqlStatement SQL statement
     * @return skipped string
     */
    public String skipParentheses(final SQLStatement sqlStatement) {
        StringBuilder result = new StringBuilder("");
        int count = 0;
        /** 如果当前token为左括号 */
        if (Symbol.LEFT_PAREN == lexer.getCurrentToken().getType()) {
            final int beginPosition = lexer.getCurrentToken().getEndPosition();
            result.append(Symbol.LEFT_PAREN.getLiterals());
            // 获取下一token
            lexer.nextToken();
            while (true) {
                // 如果是？号，增加解析索引
                if (equalAny(Symbol.QUESTION)) {
                    sqlStatement.increaseParametersIndex();
                }//  到达结尾 或者 匹配合适的)右括号
                if (Assist.END == lexer.getCurrentToken().getType() || (Symbol.RIGHT_PAREN == lexer.getCurrentToken().getType() && 0 == count)) {
                    break;
                }/** 处理里面有多个括号的情况，例如：SELECT COUNT(DISTINCT(order_id) FROM t_order */
                if (Symbol.LEFT_PAREN == lexer.getCurrentToken().getType()) {
                    count++;
                } else if (Symbol.RIGHT_PAREN == lexer.getCurrentToken().getType()) {
                    count--;
                }/** 解析下一个token */
                lexer.nextToken();
            }
            result.append(lexer.getInput().substring(beginPosition, lexer.getCurrentToken().getEndPosition()));
            lexer.nextToken();
        }
        return result.toString();
    }
    
    /**
     * Assert current token type should equals input token and go to next token type.
     *
     * @param tokenType token type
     */
    public void accept(final TokenType tokenType) {
        if (lexer.getCurrentToken().getType() != tokenType) {
            throw new SQLParsingException(lexer, tokenType);
        }
        lexer.nextToken();
    }
    
    /**
     * Adjust current token equals one of input tokens or not.
     *
     * @param tokenTypes to be adjusted token types
     * @return current token equals one of input tokens or not
     */
    public boolean equalAny(final TokenType... tokenTypes) {
        // 循环所有的不支持项，判断是否跟当前token的类型相同，相同返回true
        for (TokenType each : tokenTypes) {
            if (each == lexer.getCurrentToken().getType()) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * Skip current token if equals one of input tokens.
     * 跳过指定的token类型
     * @param tokenTypes to be adjusted token types
     * @return skipped current token or not
     */
    public boolean skipIfEqual(final TokenType... tokenTypes) {
        if (equalAny(tokenTypes)) {
            lexer.nextToken();
            return true;
        }
        return false;
    }
    
    /**
     * Skip all input tokens.
     *
     * @param tokenTypes to be skipped token types
     */
    public void skipAll(final TokenType... tokenTypes) {
        Set<TokenType> tokenTypeSet = Sets.newHashSet(tokenTypes);
        while (tokenTypeSet.contains(lexer.getCurrentToken().getType())) {
            lexer.nextToken();
        }
    }
    
    /**
     * Skip until one of input tokens.
     *
     * @param tokenTypes to be skipped untiled token types
     */
    public void skipUntil(final TokenType... tokenTypes) {
        Set<TokenType> tokenTypeSet = Sets.newHashSet(tokenTypes);
        tokenTypeSet.add(Assist.END);
        while (!tokenTypeSet.contains(lexer.getCurrentToken().getType())) {
            lexer.nextToken();
        }
    }
    
    /**
     * Throw unsupported exception if current token equals one of input tokens.
     * 抛出不支持语句异常
     * @param tokenTypes to be adjusted token types
     */
    public void unsupportedIfEqual(final TokenType... tokenTypes) {
        // 判断当前token是否包含在不支持项中
        if (equalAny(tokenTypes)) {
            // 抛出不支持异常
            throw new SQLParsingUnsupportedException(lexer.getCurrentToken().getType());
        }
    }
    
    /**
     * Throw unsupported exception if current token not equals one of input tokens.
     *
     * @param tokenTypes to be adjusted token types
     */
    public void unsupportedIfNotSkip(final TokenType... tokenTypes) {
        if (!skipIfEqual(tokenTypes)) {
            throw new SQLParsingUnsupportedException(lexer.getCurrentToken().getType());
        }
    }
    
    /**
     * Get database type.
     * 
     * @return database type
     */
    public DatabaseType getDatabaseType() {
        if (lexer instanceof H2Lexer) {
            return DatabaseType.H2;
        }
        if (lexer instanceof MySQLLexer) {
            return DatabaseType.MySQL;
        }
        if (lexer instanceof OracleLexer) {
            return DatabaseType.Oracle;
        }
        if (lexer instanceof SQLServerLexer) {
            return DatabaseType.SQLServer;
        }
        if (lexer instanceof PostgreSQLLexer) {
            return DatabaseType.PostgreSQL;
        }
        throw new UnsupportedOperationException(String.format("Cannot support lexer class: %s", lexer.getClass().getCanonicalName()));
    }
}
