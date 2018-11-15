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

import io.shardingsphere.core.parsing.lexer.analyzer.CharType;
import io.shardingsphere.core.parsing.lexer.analyzer.Dictionary;
import io.shardingsphere.core.parsing.lexer.analyzer.Tokenizer;
import io.shardingsphere.core.parsing.lexer.token.Assist;
import io.shardingsphere.core.parsing.lexer.token.Token;
import io.shardingsphere.core.parsing.parser.exception.SQLParsingException;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Lexical analysis.
 * 词法解析器
 *
 * @author zhangliang
 */
@RequiredArgsConstructor
public class Lexer {

    /**
     * 输入字符串
     */
    @Getter
    private final String input;
    /**
     * 词法标记字典
     */
    private final Dictionary dictionary;
    /**
     * 解析到SQL的offset
     */
    private int offset;
    /**
     * 当前词法标记
     */
    @Getter
    private Token currentToken;

    /**
     * Analyse next token.
     * 分析下一个词法标记
     */
    public final void nextToken() {
        // 跳过忽略的词法标记
        skipIgnoredToken();
        // 变量
        if (isVariableBegin()) {
            currentToken = new Tokenizer(input, dictionary, offset).scanVariable();
            // 是否是以N\开头
        } else if (isNCharBegin()) {
            currentToken = new Tokenizer(input, dictionary, ++offset).scanChars();
            // 是否是以字母开头
        } else if (isIdentifierBegin()) {
            currentToken = new Tokenizer(input, dictionary, offset).scanIdentifier();
            // 是否是16进制
        } else if (isHexDecimalBegin()) {
            currentToken = new Tokenizer(input, dictionary, offset).scanHexDecimal();
            // 是否是数字开头
        } else if (isNumberBegin()) {
            currentToken = new Tokenizer(input, dictionary, offset).scanNumber();
            // 符号
        } else if (isSymbolBegin()) {
            currentToken = new Tokenizer(input, dictionary, offset).scanSymbol();
            // 字符串 "ABC"
        } else if (isCharsBegin()) {
            currentToken = new Tokenizer(input, dictionary, offset).scanChars();
            // 结束
        } else if (isEnd()) {
            currentToken = new Token(Assist.END, "", offset);
            // 分析出错
        } else {
            throw new SQLParsingException(this, Assist.ERROR);
        }
        offset = currentToken.getEndPosition();
    }

    /**
     * 跳过忽略的词法标记
     * 1.空格
     * 2.sql hint
     * 3.sql 注释
     */
    private void skipIgnoredToken() {
        // 跳过空格
        offset = new Tokenizer(input, dictionary, offset).skipWhitespace();
        // 始终为false
        while (isHintBegin()) {
            offset = new Tokenizer(input, dictionary, offset).skipHint();
            offset = new Tokenizer(input, dictionary, offset).skipWhitespace();
        }
        while (isCommentBegin()) {
            offset = new Tokenizer(input, dictionary, offset).skipComment();
            offset = new Tokenizer(input, dictionary, offset).skipWhitespace();
        }
    }

    /**
     * 是否是以hint开头
     *
     * @return
     */
    protected boolean isHintBegin() {
        return false;
    }

    /**
     * 判断是否是注释
     * // -- /* 这三种表示注释
     *
     * @return
     */
    protected boolean isCommentBegin() {
        // 获取当前字符
        char current = getCurrentChar(0);
        // 获取下一个字符
        char next = getCurrentChar(1);
        // 判断是否是注释
        return '/' == current && '/' == next || '-' == current && '-' == next || '/' == current && '*' == next;
    }

    protected boolean isVariableBegin() {
        return false;
    }

    /**
     * 是否支持N\，始终返回false
     *
     * @return
     */
    protected boolean isSupportNChars() {
        return false;
    }

    /**
     * 判断是否是以N\开头
     * 目前SQLServer独有：在 SQL Server 中处理 Unicode 字串常数时，必需为所有的 Unicode 字串加上前置词N
     *
     * @return
     */
    private boolean isNCharBegin() {
        return isSupportNChars() && 'N' == getCurrentChar(0) && '\'' == getCurrentChar(1);
    }

    /**
     * 是否是标志符开头
     *
     * @return
     */
    private boolean isIdentifierBegin() {
        return isIdentifierBegin(getCurrentChar(0));
    }

    /**
     * 是否是英文字母或‘或_或$三个符号
     *
     * @param ch
     * @return
     */
    protected boolean isIdentifierBegin(final char ch) {
        return CharType.isAlphabet(ch) || '`' == ch || '_' == ch || '$' == ch;
    }

    /**
     * 是否是十六进制
     *
     * @return
     */
    private boolean isHexDecimalBegin() {
        return '0' == getCurrentChar(0) && 'x' == getCurrentChar(1);
    }

    /**
     * SELECT * FROM t_user WHERE id = 1
     * <p>
     * 是否是 数字
     * '-' 需要特殊处理。".2" 被处理成省略0的小数，"-.2" 不能被处理成省略的小数，否则会出问题。
     * 例如说，"SELECT a-.2" 处理的结果是 "SELECT" / "a" / "-" / ".2"
     *
     * @return
     */
    private boolean isNumberBegin() {
        return CharType.isDigital(getCurrentChar(0)) || ('.' == getCurrentChar(0) && CharType.isDigital(getCurrentChar(1)) && !isIdentifierBegin(getCurrentChar(-1))
                || ('-' == getCurrentChar(0) && ('.' == getCurrentChar(1) || CharType.isDigital(getCurrentChar(1)))));
    }

    /**
     * 是否是符号 {} >= 等
     * @return
     */
    private boolean isSymbolBegin() {
        return CharType.isSymbol(getCurrentChar(0));
    }

    protected boolean isCharsBegin() {
        return '\'' == getCurrentChar(0) || '\"' == getCurrentChar(0);
    }

    private boolean isEnd() {
        return offset >= input.length();
    }

    protected final char getCurrentChar(final int offset) {
        return this.offset + offset >= input.length() ? (char) CharType.EOI : input.charAt(this.offset + offset);
    }
}
