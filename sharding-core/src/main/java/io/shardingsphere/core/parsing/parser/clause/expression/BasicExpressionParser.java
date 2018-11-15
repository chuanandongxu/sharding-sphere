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

package io.shardingsphere.core.parsing.parser.clause.expression;

import io.shardingsphere.core.parsing.lexer.LexerEngine;
import io.shardingsphere.core.parsing.lexer.token.Literals;
import io.shardingsphere.core.parsing.lexer.token.Symbol;
import io.shardingsphere.core.parsing.parser.expression.SQLExpression;
import io.shardingsphere.core.parsing.parser.expression.SQLIdentifierExpression;
import io.shardingsphere.core.parsing.parser.expression.SQLIgnoreExpression;
import io.shardingsphere.core.parsing.parser.expression.SQLNumberExpression;
import io.shardingsphere.core.parsing.parser.expression.SQLPlaceholderExpression;
import io.shardingsphere.core.parsing.parser.expression.SQLPropertyExpression;
import io.shardingsphere.core.parsing.parser.expression.SQLTextExpression;
import io.shardingsphere.core.parsing.parser.sql.SQLStatement;
import io.shardingsphere.core.parsing.parser.token.TableToken;
import io.shardingsphere.core.util.NumberUtil;
import io.shardingsphere.core.util.SQLUtil;
import lombok.RequiredArgsConstructor;

/**
 * Basic expression parser.
 * 基本从句表达式
 * @author zhangliang
 */
@RequiredArgsConstructor
public final class BasicExpressionParser {
    
    private final LexerEngine lexerEngine;
    
    /**
     * Parse expression.
     * 解析表达式
     * @param sqlStatement SQL statement
     * @return expression
     */
    public SQLExpression parse(final SQLStatement sqlStatement) {
        // 获取当前token的结束位置
        int beginPosition = lexerEngine.getCurrentToken().getEndPosition();
        // 解析表达式
        SQLExpression result = parseExpression(sqlStatement);
        // 如果是属性表达式
        if (result instanceof SQLPropertyExpression) {
            setTableToken(sqlStatement, beginPosition, (SQLPropertyExpression) result);
        }
        return result;
    }

    /**
     * 解析表达式
     * @param sqlStatement
     * @return
     */
    // TODO complete more expression parse
    private SQLExpression parseExpression(final SQLStatement sqlStatement) {
        // 获取当前token的字面量标记
        String literals = lexerEngine.getCurrentToken().getLiterals();
        // 获取当前token的开始位置
        final int beginPosition = lexerEngine.getCurrentToken().getEndPosition() - literals.length();
        // 获取表达式
        final SQLExpression expression = getExpression(literals, sqlStatement);
        // 获取下一个token
        lexerEngine.nextToken();
        /** SQLIdentifierExpression 需要特殊处理。考虑自定义函数，表名.属性情况。*/
        /** 例如，ORDER BY o.uid 中的 "o.uid" */
        if (lexerEngine.skipIfEqual(Symbol.DOT)) {
            String property = lexerEngine.getCurrentToken().getLiterals();
            lexerEngine.nextToken();
            return skipIfCompositeExpression(sqlStatement)
                    ? new SQLIgnoreExpression(lexerEngine.getInput().substring(beginPosition, lexerEngine.getCurrentToken().getEndPosition()))
                    : new SQLPropertyExpression(new SQLIdentifierExpression(literals), property);
        }/** 例如，GROUP BY DATE(create_time) 中的 "DATE(create_time)"*/
        if (lexerEngine.equalAny(Symbol.LEFT_PAREN)) {
            // 跳过括号里的词法标记
            lexerEngine.skipParentheses(sqlStatement);
            skipRestCompositeExpression(sqlStatement);
            return new SQLIgnoreExpression(lexerEngine.getInput().substring(beginPosition,
                    lexerEngine.getCurrentToken().getEndPosition() - lexerEngine.getCurrentToken().getLiterals().length()).trim());
        }/** 如果是复合表达式则跳过 */
        return skipIfCompositeExpression(sqlStatement)
                ? new SQLIgnoreExpression(lexerEngine.getInput().substring(beginPosition, lexerEngine.getCurrentToken().getEndPosition())) : expression;
    }

    /**
     * 根据字面量标记和sqlstatement获取表达式
     * @param literals
     * @param sqlStatement
     * @return
     */
    private SQLExpression getExpression(final String literals, final SQLStatement sqlStatement) {
        /**
         * 如果是？号，创建占位符表达式并返回
         */
        if (lexerEngine.equalAny(Symbol.QUESTION)) {
            sqlStatement.increaseParametersIndex();
            return new SQLPlaceholderExpression(sqlStatement.getParametersIndex() - 1);
        }// 字符
        if (lexerEngine.equalAny(Literals.CHARS)) {
            return new SQLTextExpression(literals);
        }// int
        if (lexerEngine.equalAny(Literals.INT)) {
            return new SQLNumberExpression(NumberUtil.getExactlyNumber(literals, 10));
        }// 十六进制
        if (lexerEngine.equalAny(Literals.FLOAT)) {
            return new SQLNumberExpression(Double.parseDouble(literals));
        }
        if (lexerEngine.equalAny(Literals.HEX)) {
            return new SQLNumberExpression(NumberUtil.getExactlyNumber(literals, 16));
        }// 标志符
        if (lexerEngine.equalAny(Literals.IDENTIFIER)) {
            return new SQLIdentifierExpression(SQLUtil.getExactlyValue(literals));
        }// 其他则忽略
        return new SQLIgnoreExpression(literals);
    }

    /**
     * 如果是 复合表达式，跳过。
     * @param sqlStatement
     * @return
     */
    private boolean skipIfCompositeExpression(final SQLStatement sqlStatement) {
        if (lexerEngine.equalAny(
                Symbol.PLUS, Symbol.SUB, Symbol.STAR, Symbol.SLASH, Symbol.PERCENT, Symbol.AMP, Symbol.BAR, Symbol.DOUBLE_AMP, Symbol.DOUBLE_BAR, Symbol.CARET, Symbol.DOT, Symbol.LEFT_PAREN)) {
            lexerEngine.skipParentheses(sqlStatement);
            skipRestCompositeExpression(sqlStatement);
            return true;
        }
        if ((Literals.INT == lexerEngine.getCurrentToken().getType() || Literals.FLOAT == lexerEngine.getCurrentToken().getType()) && lexerEngine.getCurrentToken().getLiterals().startsWith("-")) {
            lexerEngine.nextToken();
            return true;
        }
        return false;
    }

    /**
     * 跳过剩余复合表达式
     * @param sqlStatement
     */
    private void skipRestCompositeExpression(final SQLStatement sqlStatement) {
        while (lexerEngine.skipIfEqual(Symbol.PLUS, Symbol.SUB, Symbol.STAR, Symbol.SLASH, Symbol.PERCENT, Symbol.AMP, Symbol.BAR, Symbol.DOUBLE_AMP, Symbol.DOUBLE_BAR, Symbol.CARET, Symbol.DOT)) {
            if (lexerEngine.equalAny(Symbol.QUESTION)) {
                sqlStatement.increaseParametersIndex();
            }
            lexerEngine.nextToken();
            lexerEngine.skipParentheses(sqlStatement);
        }
    }
    
    private void setTableToken(final SQLStatement sqlStatement, final int beginPosition, final SQLPropertyExpression propertyExpr) {
        String owner = propertyExpr.getOwner().getName();
        if (sqlStatement.getTables().getTableNames().contains(SQLUtil.getExactlyValue(propertyExpr.getOwner().getName()))) {
            sqlStatement.addSQLToken(new TableToken(beginPosition - owner.length(), 0, owner));
        }
    }
}
