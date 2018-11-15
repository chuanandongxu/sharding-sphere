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

package io.shardingsphere.core.parsing.lexer.analyzer;

import io.shardingsphere.core.parsing.lexer.token.DefaultKeyword;
import io.shardingsphere.core.parsing.lexer.token.Keyword;
import io.shardingsphere.core.parsing.lexer.token.TokenType;

import java.util.HashMap;
import java.util.Map;

/**
 * Token dictionary.
 *
 * @author zhangliang
 */
public final class Dictionary {
    /**
     * 词法关键词map
     */
    private final Map<String, Keyword> tokens = new HashMap<>(1024);

    public Dictionary(final Keyword... dialectKeywords) {
        fill(dialectKeywords);
    }

    /**
     * 装上默认词法关键词 + 方言词法关键词
     * 不同的数据库有相同的默认词法关键词，有不同的方言关键词
     *
     * @param dialectKeywords
     */
    private void fill(final Keyword... dialectKeywords) {
        // 默认关键词
        for (DefaultKeyword each : DefaultKeyword.values()) {
            tokens.put(each.name(), each);
        }
        // 方言关键词
        for (Keyword each : dialectKeywords) {
            tokens.put(each.toString(), each);
        }
    }

    /**
     * 判断是否包含关键词，包含则返回，不包含则使用默认的
     * @param literals
     * @param defaultTokenType
     * @return
     */
    TokenType findTokenType(final String literals, final TokenType defaultTokenType) {
        String key = null == literals ? null : literals.toUpperCase();
        return tokens.containsKey(key) ? tokens.get(key) : defaultTokenType;
    }

    /**
     * 根据标志符获取词法标记类型
     * @param literals
     * @return
     */
    TokenType findTokenType(final String literals) {
        String key = null == literals ? null : literals.toUpperCase();
        if (tokens.containsKey(key)) {
            return tokens.get(key);
        }
        throw new IllegalArgumentException();
    }
}
