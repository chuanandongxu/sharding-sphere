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
import com.google.common.base.Preconditions;
import io.shardingsphere.core.metadata.table.ShardingTableMetaData;
import io.shardingsphere.core.parsing.lexer.LexerEngine;
import io.shardingsphere.core.parsing.lexer.token.Assist;
import io.shardingsphere.core.parsing.lexer.token.Symbol;
import io.shardingsphere.core.parsing.parser.clause.expression.BasicExpressionParser;
import io.shardingsphere.core.parsing.parser.context.condition.Column;
import io.shardingsphere.core.parsing.parser.dialect.ExpressionParserFactory;
import io.shardingsphere.core.parsing.parser.expression.SQLExpression;
import io.shardingsphere.core.parsing.parser.expression.SQLIdentifierExpression;
import io.shardingsphere.core.parsing.parser.expression.SQLIgnoreExpression;
import io.shardingsphere.core.parsing.parser.expression.SQLPropertyExpression;
import io.shardingsphere.core.parsing.parser.sql.dml.insert.InsertStatement;
import io.shardingsphere.core.parsing.parser.token.InsertColumnToken;
import io.shardingsphere.core.parsing.parser.token.ItemsToken;
import io.shardingsphere.core.rule.ShardingRule;
import io.shardingsphere.core.util.SQLUtil;

import java.util.Collection;
import java.util.LinkedList;

/**
 * Insert columns clause parser.
 *
 * @author zhangliang
 * @author maxiaoguang
 * @author panjuan
 */
public final class InsertColumnsClauseParser implements SQLClauseParser {
    
    private final ShardingRule shardingRule;
    
    private final LexerEngine lexerEngine;
    
    private final BasicExpressionParser basicExpressionParser;
    
    public InsertColumnsClauseParser(final ShardingRule shardingRule, final LexerEngine lexerEngine) {
        this.shardingRule = shardingRule;
        this.lexerEngine = lexerEngine;
        basicExpressionParser = ExpressionParserFactory.createBasicExpressionParser(lexerEngine);
    }
    
    /**
     * Parse insert columns.
     *
     * @param insertStatement insert statement
     * @param shardingTableMetaData sharding table meta data
     */
    public void parse(final InsertStatement insertStatement, final ShardingTableMetaData shardingTableMetaData) {
        /** 获取表名 只获取第一个*/
        String tableName = insertStatement.getTables().getSingleTableName();
        /** 根据表名获取分布式id 的列 */
        Optional<Column> generateKeyColumn = shardingRule.getGenerateKeyColumn(tableName);
        /** 如果当前token是( 则解析()中的cloumn,,否则直接解析cloumn */
        insertStatement.getColumns().addAll(lexerEngine.equalAny(Symbol.LEFT_PAREN)
                ? parseWithColumn(insertStatement, tableName, generateKeyColumn) : parseWithoutColumn(insertStatement, shardingTableMetaData, tableName, generateKeyColumn));
    }

    /**
     * 解析() 中的cloumn
     * @param insertStatement
     * @param tableName
     * @param generateKeyColumn
     * @return
     */
    private Collection<Column> parseWithColumn(final InsertStatement insertStatement, final String tableName, final Optional<Column> generateKeyColumn) {
        int count = 0;
        Collection<Column> result = new LinkedList<>();
        do {/** 如果不是 )或者语句结束，则解析所有的cloumn */
            lexerEngine.nextToken();
            SQLExpression sqlExpression = basicExpressionParser.parse(insertStatement);
            String columnName = null;
            // 属性表达式
            if (sqlExpression instanceof SQLPropertyExpression) {
                columnName = SQLUtil.getExactlyValue(((SQLPropertyExpression) sqlExpression).getName());
            }// 标志符表达式
            if (sqlExpression instanceof SQLIdentifierExpression) {
                columnName = SQLUtil.getExactlyValue(((SQLIdentifierExpression) sqlExpression).getName());
            }// 分片中忽略的表达式
            if (sqlExpression instanceof SQLIgnoreExpression) {
                columnName = SQLUtil.getExactlyValue(((SQLIgnoreExpression) sqlExpression).getExpression());
            }// 非空验证
            Preconditions.checkNotNull(columnName);
            result.add(new Column(columnName, tableName));
            /** 分布式id存在， 设置generateKeyColumnIndex为count */
            if (generateKeyColumn.isPresent() && generateKeyColumn.get().getName().equalsIgnoreCase(columnName)) {
                insertStatement.setGenerateKeyColumnIndex(count);
            }
            count++;
        } while (!lexerEngine.equalAny(Symbol.RIGHT_PAREN) && !lexerEngine.equalAny(Assist.END));
        insertStatement.setColumnsListLastPosition(lexerEngine.getCurrentToken().getEndPosition() - lexerEngine.getCurrentToken().getLiterals().length());
        lexerEngine.nextToken();
        return result;
    }

    /**
     * 解析不带括号的cloumn
     * @param insertStatement
     * @param shardingTableMetaData
     * @param tableName
     * @param generateKeyColumn
     * @return
     */
    private Collection<Column> parseWithoutColumn(
            final InsertStatement insertStatement, final ShardingTableMetaData shardingTableMetaData, final String tableName, final Optional<Column> generateKeyColumn) {
        int count = 0;
        /** 获取开始位置 */
        int beginPosition = lexerEngine.getCurrentToken().getEndPosition() - lexerEngine.getCurrentToken().getLiterals().length() - 1;
        /** 增加 ( */
        insertStatement.addSQLToken(new InsertColumnToken(beginPosition, "("));
        ItemsToken columnsToken = new ItemsToken(beginPosition);
        columnsToken.setFirstOfItemsSpecial(true);
        Collection<Column> result = new LinkedList<>();
        /** 如果分表元数据中包含该表 */
        if (shardingTableMetaData.containsTable(tableName)) {
            /** 从元数据中获取所有列的名字 ,创建column */
            for (String each : shardingTableMetaData.getAllColumnNames(tableName)) {
                result.add(new Column(each, tableName));
                /** 判断分布式ID 是否存在 */
                if (generateKeyColumn.isPresent() && generateKeyColumn.get().getName().equalsIgnoreCase(each)) {
                    insertStatement.setGenerateKeyColumnIndex(count);
                }
                columnsToken.getItems().add(each);
                count++;
            }
        }
        /** 加入解析的cloumns */
        insertStatement.addSQLToken(columnsToken);
        /** 添加 ) */
        insertStatement.addSQLToken(new InsertColumnToken(beginPosition, ")"));
        /** 设置解析的 (column1, column )的起始位置 */
        insertStatement.setColumnsListLastPosition(beginPosition);
        return result;
    }
}
