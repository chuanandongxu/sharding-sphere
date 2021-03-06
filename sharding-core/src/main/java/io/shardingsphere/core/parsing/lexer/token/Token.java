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

package io.shardingsphere.core.parsing.lexer.token;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Token.
 *
 * @author zhangliang
 */
@RequiredArgsConstructor
@Getter
public final class Token {
    /**
     * 词法标记的类型
     */
    private final TokenType type;
    /**
     * 词法字面量标记
     */
    private final String literals;
    /**
     * literals 在 SQL 里的结束位置
     */
    private final int endPosition;
}
