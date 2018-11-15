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
import io.shardingsphere.core.parsing.lexer.token.Literals;
import io.shardingsphere.core.parsing.lexer.token.Symbol;
import io.shardingsphere.core.parsing.lexer.token.Token;
import io.shardingsphere.core.parsing.lexer.token.TokenType;
import lombok.RequiredArgsConstructor;

/**
 * Tokenizer.
 *
 * @author zhangliang
 */
@RequiredArgsConstructor
public final class Tokenizer {
    
    private static final int MYSQL_SPECIAL_COMMENT_BEGIN_SYMBOL_LENGTH = 1;
    
    private static final int COMMENT_BEGIN_SYMBOL_LENGTH = 2;
    
    private static final int HINT_BEGIN_SYMBOL_LENGTH = 3;
    
    private static final int COMMENT_AND_HINT_END_SYMBOL_LENGTH = 2;
    
    private static final int HEX_BEGIN_SYMBOL_LENGTH = 2;
    
    private final String input;
    
    private final Dictionary dictionary;
    
    private final int offset;
    
    /**
     * skip whitespace.
     * 跳过空格
     * @return offset after whitespace skipped 
     */
    public int skipWhitespace() {
        int length = 0;
        while (CharType.isWhitespace(charAt(offset + length))) {
            length++;
        }
        return offset + length;
    }
    
    /**
     * skip comment.
     * 
     * @return offset after comment skipped
     */
    public int skipComment() {
        char current = charAt(offset);
        char next = charAt(offset + 1);
        if (isSingleLineCommentBegin(current, next)) {
            return skipSingleLineComment(COMMENT_BEGIN_SYMBOL_LENGTH);
        } else if ('#' == current) {
            return skipSingleLineComment(MYSQL_SPECIAL_COMMENT_BEGIN_SYMBOL_LENGTH);
        } else if (isMultipleLineCommentBegin(current, next)) {
            return skipMultiLineComment();
        }
        return offset;
    }
    
    private boolean isSingleLineCommentBegin(final char ch, final char next) {
        return '/' == ch && '/' == next || '-' == ch && '-' == next;
    }
    
    private int skipSingleLineComment(final int commentSymbolLength) {
        int length = commentSymbolLength;
        while (!CharType.isEndOfInput(charAt(offset + length)) && '\n' != charAt(offset + length)) {
            length++;
        }
        return offset + length + 1;
    }
    
    private boolean isMultipleLineCommentBegin(final char ch, final char next) {
        return '/' == ch && '*' == next;
    }
    
    private int skipMultiLineComment() {
        return untilCommentAndHintTerminateSign(COMMENT_BEGIN_SYMBOL_LENGTH);
    }
    
    /**
     * skip hint.
     *
     * @return offset after hint skipped
     */
    public int skipHint() {
        return untilCommentAndHintTerminateSign(HINT_BEGIN_SYMBOL_LENGTH);
    }
    
    private int untilCommentAndHintTerminateSign(final int beginSymbolLength) {
        int length = beginSymbolLength;
        while (!isMultipleLineCommentEnd(charAt(offset + length), charAt(offset + length + 1))) {
            if (CharType.isEndOfInput(charAt(offset + length))) {
                throw new UnterminatedCharException("*/");
            }
            length++;
        }
        return offset + length + COMMENT_AND_HINT_END_SYMBOL_LENGTH;
    }
    
    private boolean isMultipleLineCommentEnd(final char ch, final char next) {
        return '*' == ch && '/' == next;
    }
    
    /**
     * scan variable.
     * 扫描变量 在MySQL里，@代表用户变量；@@代表系统变量。
     * @return variable token
     */
    public Token scanVariable() {
        int length = 1;
        if ('@' == charAt(offset + 1)) {
            length++;
        }
        // 直到第一个不是变量的符号
        while (isVariableChar(charAt(offset + length))) {
            length++;
        }
        // 截取字符串生成token
        return new Token(Literals.VARIABLE, input.substring(offset, offset + length), offset + length);
    }

    /**
     * 是否是变量符号
     * a-z A-Z 0-9 _ $ # . 都是
     * @param ch
     * @return
     */
    private boolean isVariableChar(final char ch) {
        return isIdentifierChar(ch) || '.' == ch;
    }
    
    /**
     * scan identifier.
     * 扫描标志符
     * @return identifier token
     */
    public Token scanIdentifier() {
        // `字段`，例如：SELECT `id` FROM t_user 中的 `id`
        if ('`' == charAt(offset)) {
            // 计算到第二个"'"的长度
            int length = getLengthUntilTerminatedChar('`');
            // 创建标志符token及偏移量
            return new Token(Literals.IDENTIFIER, input.substring(offset, offset + length), offset + length);
        }
        // "字段"
        if ('"' == charAt(offset)) {
            int length = getLengthUntilTerminatedChar('"');
            return new Token(Literals.IDENTIFIER, input.substring(offset, offset + length), offset + length);
        }
        // [字段]
        if ('[' == charAt(offset)) {
            int length = getLengthUntilTerminatedChar(']');
            return new Token(Literals.IDENTIFIER, input.substring(offset, offset + length), offset + length);
        }
        int length = 0;
        // 是标志符则继续循环
        while (isIdentifierChar(charAt(offset + length))) {
            length++;
        }
        // 截取
        String literals = input.substring(offset, offset + length);
        // 判断是否是order或group
        if (isAmbiguousIdentifier(literals)) {
            return new Token(processAmbiguousIdentifier(offset + length, literals), literals, offset + length);
        }
        // 创建token
        return new Token(dictionary.findTokenType(literals, Literals.IDENTIFIER), literals, offset + length);
    }

    /**
     * 获取结束符的之间的长度
     * 处理类似 SELECT a AS `b``c` FROM table。此处连续的 "``" 不是结尾，如果传递的是 "`" 会产生误判，所以加了这个判断
     * @param terminatedChar
     * @return
     */
    private int getLengthUntilTerminatedChar(final char terminatedChar) {
        int length = 1;
        while (terminatedChar != charAt(offset + length) || hasEscapeChar(terminatedChar, offset + length)) {
            // 超过输入字符串的长度
            if (offset + length >= input.length()) {
                throw new UnterminatedCharException(terminatedChar);
            }
            if (hasEscapeChar(terminatedChar, offset + length)) {
                length++;
            }
            length++;
        }
        return length + 1;
    }

    /**
     * 处理类似 SELECT a AS `b``c` FROM table。此处连续的 "``" 不是结尾，如果传递的是 "`" 会产生误判，所以加了这个判断
     * offset 及 offset+1 的字符都等于charIdentifier
     * @param charIdentifier
     * @param offset
     * @return
     */
    private boolean hasEscapeChar(final char charIdentifier, final int offset) {
        return charIdentifier == charAt(offset) && charIdentifier == charAt(offset + 1);
    }

    /**
     * 判断是否是标志符
     * a-z A-Z 0-9 _ $ # 都是
     * @param ch
     * @return
     */
    private boolean isIdentifierChar(final char ch) {
        return CharType.isAlphabet(ch) || CharType.isDigital(ch) || '_' == ch || '$' == ch || '#' == ch;
    }

    /**
     * 判断截取的字符是否是order 获取group
     * 是否是引起歧义的标识符
     * 例如 "SELECT * FROM group"，此时 "group" 代表的是表名，而非词法关键词
     * @param literals 标志符
     * @return
     */
    private boolean isAmbiguousIdentifier(final String literals) {
        return DefaultKeyword.ORDER.name().equalsIgnoreCase(literals) || DefaultKeyword.GROUP.name().equalsIgnoreCase(literals);
    }

    /**
     * 获取引起歧义的标识符对应的词法标记类型
     * @param offset
     * @param literals 标志符
     * @return
     */
    private TokenType processAmbiguousIdentifier(final int offset, final String literals) {
        int i = 0;
        // 略过空格
        while (CharType.isWhitespace(charAt(offset + i))) {
            i++;
        }
        // 判断下一个标志符是否是by
        if (DefaultKeyword.BY.name().equalsIgnoreCase(String.valueOf(new char[] {charAt(offset + i), charAt(offset + i + 1)}))) {
            // 返回词法标记类型
            return dictionary.findTokenType(literals);
        }
        // 否则返回标志符标记类型 如作为表名
        return Literals.IDENTIFIER;
    }
    
    /**
     * scan hex decimal.
     * 扫描十六进制数
     * @return hex decimal token
     */
    public Token scanHexDecimal() {
        // 0x 占两个符
        int length = HEX_BEGIN_SYMBOL_LENGTH;
        // 负数
        if ('-' == charAt(offset + length)) {
            length++;
        }
        while (isHex(charAt(offset + length))) {
            length++;
        }
        // 截取十六进制数字构造token
        return new Token(Literals.HEX, input.substring(offset, offset + length), offset + length);
    }

    /**
     * A--F a--f 0-9
     * @param ch
     * @return
     */
    private boolean isHex(final char ch) {
        return ch >= 'A' && ch <= 'F' || ch >= 'a' && ch <= 'f' || CharType.isDigital(ch);
    }
    
    /**
     * scan number.
     * 扫描数字
     * 解析数字的结果会有两种：整数 和 浮点数.
     * @return number token
     */
    public Token scanNumber() {
        int length = 0;
        // 负数
        if ('-' == charAt(offset + length)) {
            length++;
        }
        // 计算数字长度
        length += getDigitalLength(offset + length);
        boolean isFloat = false;
        // float 类型
        if ('.' == charAt(offset + length)) {
            isFloat = true;
            length++;
            length += getDigitalLength(offset + length);
        }
        // 科学计数表示，例如：SELECT 7.823E5
        if (isScientificNotation(offset + length)) {
            isFloat = true;
            length++;
            if ('+' == charAt(offset + length) || '-' == charAt(offset + length)) {
                length++;
            }
            length += getDigitalLength(offset + length);
        }
        // 浮点数，例如：SELECT 1.333F
        if (isBinaryNumber(offset + length)) {
            isFloat = true;
            length++;
        }
        return new Token(isFloat ? Literals.FLOAT : Literals.INT, input.substring(offset, offset + length), offset + length);
    }

    /**
     * 计算数字的长度
     * @param offset
     * @return
     */
    private int getDigitalLength(final int offset) {
        int result = 0;
        while (CharType.isDigital(charAt(offset + result))) {
            result++;
        }
        return result;
    }

    /**
     * 科学计数表示，例如：SELECT 7.823E5
     * @param offset
     * @return
     */
    private boolean isScientificNotation(final int offset) {
        char current = charAt(offset);
        return 'e' == current || 'E' == current;
    }

    /**
     * 浮点数，例如：SELECT 1.333F
     * @param offset
     * @return
     */
    private boolean isBinaryNumber(final int offset) {
        char current = charAt(offset);
        return 'f' == current || 'F' == current || 'd' == current || 'D' == current;
    }
    
    /**
     * scan chars.
     * 扫描字符串
     * @return chars token
     */
    public Token scanChars() {
        return scanChars(charAt(offset));
    }
    
    private Token scanChars(final char terminatedChar) {
        int length = getLengthUntilTerminatedChar(terminatedChar);
        return new Token(Literals.CHARS, input.substring(offset + 1, offset + length - 1), offset + length);
    }
    
    /**
     * scan symbol.
     * 扫描符号
     * {} >= 等
     * @return symbol token
     */
    public Token scanSymbol() {
        int length = 0;
        while (CharType.isSymbol(charAt(offset + length))) {
            length++;
        }
        String literals = input.substring(offset, offset + length);
        // 倒序遍历，查询符合条件的符号。例如 literals = ";;"，会是拆分成两个 ";"。
        // 如果基于正序，literals = "<="，会被解析成 "<" + "="。
        Symbol symbol;
        while (null == (symbol = Symbol.literalsOf(literals))) {
            literals = input.substring(offset, offset + --length);
        }
        return new Token(symbol, literals, offset + length);
    }

    /**
     * 获取输入SQL字符串的指定位置的字符
     * @param index
     * @return
     */
    private char charAt(final int index) {
        return index >= input.length() ? (char) CharType.EOI : input.charAt(index);
    }
}
