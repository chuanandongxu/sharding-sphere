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

package io.shardingsphere.core.parsing.lexer.dialect.mysql;

import io.shardingsphere.core.parsing.lexer.Lexer;
import io.shardingsphere.core.parsing.lexer.analyzer.Dictionary;

/**
 * MySQL Lexical analysis.
 *
 * @author zhangliang
 */
public final class MySQLLexer extends Lexer {
    /**
     * 根据枚举创建字典
     */
    private static Dictionary dictionary = new Dictionary(MySQLKeyword.values());

    /**
     * 通过构造创建词法解析器对象
     * @param input
     */
    public MySQLLexer(final String input) {
        super(input, dictionary);
    }

    /**
     * 重写isHintBegin方法
     *  /*! ---> 开头表示hint sql
     * @return
     */
    @Override
    protected boolean isHintBegin() {
        return '/' == getCurrentChar(0) && '*' == getCurrentChar(1) && '!' == getCurrentChar(2);
    }

    /**
     * # // -- /* 这四种表示注释
     * @return
     */
    @Override
    protected boolean isCommentBegin() {
        return '#' == getCurrentChar(0) || super.isCommentBegin();
    }

    /**
     * @ 开头表示是变量
     * @return
     */
    @Override
    protected boolean isVariableBegin() {
        return '@' == getCurrentChar(0);
    }
}
