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

package io.shardingsphere.core.parsing.parser.dialect.mysql.sql;

import io.shardingsphere.core.metadata.table.ShardingTableMetaData;
import io.shardingsphere.core.parsing.lexer.LexerEngine;
import io.shardingsphere.core.parsing.parser.dialect.mysql.clause.MySQLLimitClauseParser;
import io.shardingsphere.core.parsing.parser.dialect.mysql.clause.MySQLSelectOptionClauseParser;
import io.shardingsphere.core.parsing.parser.dialect.mysql.clause.facade.MySQLSelectClauseParserFacade;
import io.shardingsphere.core.parsing.parser.sql.dql.select.AbstractSelectParser;
import io.shardingsphere.core.parsing.parser.sql.dql.select.SelectStatement;
import io.shardingsphere.core.rule.ShardingRule;

/**
 * Select parser for MySQL.
 *
 * @author zhangliang
 */
public final class MySQLSelectParser extends AbstractSelectParser {
    /**
     * option解析
     */
    private final MySQLSelectOptionClauseParser selectOptionClauseParser;
    /**
     * limit解析
     */
    private final MySQLLimitClauseParser limitClauseParser;

    /**
     * 构造函数
     * @param shardingRule
     * @param lexerEngine
     * @param shardingTableMetaData
     */
    public MySQLSelectParser(final ShardingRule shardingRule, final LexerEngine lexerEngine, final ShardingTableMetaData shardingTableMetaData) {
        super(shardingRule, lexerEngine, new MySQLSelectClauseParserFacade(shardingRule, lexerEngine), shardingTableMetaData);
        selectOptionClauseParser = new MySQLSelectOptionClauseParser(lexerEngine);
        limitClauseParser = new MySQLLimitClauseParser(lexerEngine);
    }

    /**
     * 解析select SQL语句
     * @param selectStatement
     */
    @Override
    protected void parseInternal(final SelectStatement selectStatement) {
        // 解析distinct
        parseDistinct();
        // 解析option从句
        parseSelectOption();
        // 解析select 列表
        parseSelectList(selectStatement, getItems());
        // 解析from
        parseFrom(selectStatement);
        // 解析where
        parseWhere(getShardingRule(), selectStatement, getItems());
        // 解析group by
        parseGroupBy(selectStatement);
        // 解析having
        parseHaving();
        // 解析order by
        parseOrderBy(selectStatement);
        // 解析limit
        parseLimit(selectStatement);
        // 解析Union except等
        parseSelectRest();
    }

    /**
     * 解析option
     */
    private void parseSelectOption() {
        selectOptionClauseParser.parse();
    }

    /**
     * 解析limit
     * @param selectStatement
     */
    private void parseLimit(final SelectStatement selectStatement) {
        limitClauseParser.parse(selectStatement);
    }
}
