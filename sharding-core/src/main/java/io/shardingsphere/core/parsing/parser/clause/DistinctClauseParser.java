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
import lombok.RequiredArgsConstructor;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;

/**
 * Distinct clause parser.
 *
 * @author zhangliang
 */
@RequiredArgsConstructor
public abstract class DistinctClauseParser implements SQLClauseParser {
    
    private final LexerEngine lexerEngine;
    
    /**
     * Parse distinct.
     */
    public final void parse() {
        // 跳过指定的token 跳过all
        lexerEngine.skipAll(DefaultKeyword.ALL);
        Collection<Keyword> distinctKeywords = new LinkedList<>();
        // 关键词中加入distinct
        distinctKeywords.add(DefaultKeyword.DISTINCT);
        distinctKeywords.addAll(Arrays.asList(getSynonymousKeywordsForDistinct()));
        lexerEngine.unsupportedIfEqual(distinctKeywords.toArray(new Keyword[distinctKeywords.size()]));
    }

    /**
     * 不同数据不同是实现 返回Distinct keyword
     * @return
     */
    protected abstract Keyword[] getSynonymousKeywordsForDistinct();
}
