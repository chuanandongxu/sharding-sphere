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

package io.shardingsphere.core.parsing.parser.clause;

import com.google.common.base.Optional;
import io.shardingsphere.core.constant.OrderDirection;
import io.shardingsphere.core.parsing.lexer.LexerEngine;
import io.shardingsphere.core.parsing.lexer.dialect.oracle.OracleKeyword;
import io.shardingsphere.core.parsing.lexer.token.DefaultKeyword;
import io.shardingsphere.core.parsing.lexer.token.Symbol;
import io.shardingsphere.core.parsing.parser.clause.expression.BasicExpressionParser;
import io.shardingsphere.core.parsing.parser.context.OrderItem;
import io.shardingsphere.core.parsing.parser.dialect.ExpressionParserFactory;
import io.shardingsphere.core.parsing.parser.exception.SQLParsingException;
import io.shardingsphere.core.parsing.parser.expression.SQLExpression;
import io.shardingsphere.core.parsing.parser.expression.SQLIdentifierExpression;
import io.shardingsphere.core.parsing.parser.expression.SQLIgnoreExpression;
import io.shardingsphere.core.parsing.parser.expression.SQLNumberExpression;
import io.shardingsphere.core.parsing.parser.expression.SQLPlaceholderExpression;
import io.shardingsphere.core.parsing.parser.expression.SQLPropertyExpression;
import io.shardingsphere.core.parsing.parser.expression.SQLTextExpression;
import io.shardingsphere.core.parsing.parser.sql.dql.select.SelectStatement;
import io.shardingsphere.core.util.SQLUtil;
import lombok.Getter;

import java.util.LinkedList;
import java.util.List;

/**
 * Order by clause parser.
 *
 * @author zhangliang
 */
public abstract class OrderByClauseParser implements SQLClauseParser {
    
    @Getter
    private final LexerEngine lexerEngine;
    /**
     * 解析表达式的对象
     */
    private final BasicExpressionParser basicExpressionParser;

    /**
     * 创建解析表达式的对象
     * @param lexerEngine
     */
    public OrderByClauseParser(final LexerEngine lexerEngine) {
        this.lexerEngine = lexerEngine;
        basicExpressionParser = ExpressionParserFactory.createBasicExpressionParser(lexerEngine);
    }
    
    /**
     * Parse order by.
     *
     * @param selectStatement select statement
     */
    public final void parse(final SelectStatement selectStatement) {
        /** 如果是ORDER 则直接返回 */
        if (!lexerEngine.skipIfEqual(DefaultKeyword.ORDER)) {
            return;
        }
        List<OrderItem> result = new LinkedList<>();

        lexerEngine.skipIfEqual(OracleKeyword.SIBLINGS);
        /** 判断是否是by 并且调到下一个token **/
        lexerEngine.accept(DefaultKeyword.BY);
        do {
            Optional<OrderItem> orderItem = parseSelectOrderByItem(selectStatement);
            if (orderItem.isPresent()) {
                result.add(orderItem.get());
            }/** 如果是 , 则解析每一个字段 */
        } while (lexerEngine.skipIfEqual(Symbol.COMMA));
        selectStatement.getOrderByItems().addAll(result);
    }

    /**
     * 解析order by a,b,c 中的a,b,c
     * @param selectStatement
     * @return
     */
    private Optional<OrderItem> parseSelectOrderByItem(final SelectStatement selectStatement) {
        /** 解析该表达式 **/
        SQLExpression sqlExpression = basicExpressionParser.parse(selectStatement);
        /** 获取排序类型 */
        OrderDirection orderDirection = OrderDirection.ASC;
        if (lexerEngine.skipIfEqual(DefaultKeyword.ASC)) {
            orderDirection = OrderDirection.ASC;
        } else if (lexerEngine.skipIfEqual(DefaultKeyword.DESC)) {
            orderDirection = OrderDirection.DESC;
        }
        /** 字符表达式 */
        if (sqlExpression instanceof SQLTextExpression) {
            return Optional.of(new OrderItem(SQLUtil.getExactlyValue(((SQLTextExpression) sqlExpression).getText()), orderDirection, getNullOrderDirection()));
        }/** 如果是数字表达式 */
        if (sqlExpression instanceof SQLNumberExpression) {
            return Optional.of(new OrderItem(((SQLNumberExpression) sqlExpression).getNumber().intValue(), orderDirection, getNullOrderDirection()));
        }/** 标识表达式 */
        if (sqlExpression instanceof SQLIdentifierExpression) {
            return Optional.of(new OrderItem(SQLUtil.getExactlyValue(((SQLIdentifierExpression) sqlExpression).getName()),
                    orderDirection, getNullOrderDirection(), selectStatement.getAlias(SQLUtil.getExactlyValue(((SQLIdentifierExpression) sqlExpression).getName()))));
        }/** 属性表达式 */
        if (sqlExpression instanceof SQLPropertyExpression) {
            SQLPropertyExpression sqlPropertyExpression = (SQLPropertyExpression) sqlExpression;
            return Optional.of(
                new OrderItem(SQLUtil.getExactlyValue(sqlPropertyExpression.getOwner().getName()), SQLUtil.getExactlyValue(sqlPropertyExpression.getName()), orderDirection, getNullOrderDirection(),
                    selectStatement.getAlias(SQLUtil.getExactlyValue(sqlPropertyExpression.getOwner().getName()) + "." + SQLUtil.getExactlyValue(sqlPropertyExpression.getName()))));
        }/** 忽略的表达式 */
        if (sqlExpression instanceof SQLIgnoreExpression) {
            SQLIgnoreExpression sqlIgnoreExpression = (SQLIgnoreExpression) sqlExpression;
            return Optional.of(new OrderItem(sqlIgnoreExpression.getExpression(), orderDirection, getNullOrderDirection(), selectStatement.getAlias(sqlIgnoreExpression.getExpression())));
        }/** 占位符表达式 */
        if (sqlExpression instanceof SQLPlaceholderExpression) {
            return Optional.absent();
        }
        throw new SQLParsingException(lexerEngine);
    }
    
    protected abstract OrderDirection getNullOrderDirection();
}
