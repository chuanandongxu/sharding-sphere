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

import io.shardingsphere.core.parsing.lexer.LexerEngine;
import io.shardingsphere.core.parsing.lexer.token.DefaultKeyword;
import io.shardingsphere.core.parsing.lexer.token.Keyword;
import io.shardingsphere.core.parsing.lexer.token.Symbol;
import io.shardingsphere.core.parsing.parser.sql.dml.insert.InsertStatement;
import lombok.RequiredArgsConstructor;

/**
 * Insert into clause parser.
 *
 * @author zhangliang
 */
@RequiredArgsConstructor
public abstract class InsertIntoClauseParser implements SQLClauseParser {
    
    private final LexerEngine lexerEngine;
    
    private final TableReferencesClauseParser tableReferencesClauseParser;
    
    /**
     * Parse insert into.
     *
     * @param insertStatement insert statement
     */
    public void parse(final InsertStatement insertStatement) {
        /** 解析不支持项 */
        lexerEngine.unsupportedIfEqual(getUnsupportedKeywordsBeforeInto());
        /** 找到 into 关键字 */
        lexerEngine.skipUntil(DefaultKeyword.INTO);
        lexerEngine.nextToken();
        /** 解析表名 */
        tableReferencesClauseParser.parse(insertStatement, true);
        /** 忽略在表名和 value之间的关键字
         * 例如 MySQL ：[PARTITION (partition_name,...)]
         */
        skipBetweenTableAndValues(insertStatement);
    }

    /**
     * 获取insert into 不支持的关键字
     * @return
     */
    protected abstract Keyword[] getUnsupportedKeywordsBeforeInto();
    
    private void skipBetweenTableAndValues(final InsertStatement insertStatement) {
        /** 跳过partition 关键字 */
        while (lexerEngine.skipIfEqual(getSkippedKeywordsBetweenTableAndValues())) {
            lexerEngine.nextToken();
            /** 若果是 ( ,跳过小括号内所有的词法标记*/
            if (lexerEngine.equalAny(Symbol.LEFT_PAREN)) {
                lexerEngine.skipParentheses(insertStatement);
            }
        }
    }
    
    protected abstract Keyword[] getSkippedKeywordsBetweenTableAndValues();
}
