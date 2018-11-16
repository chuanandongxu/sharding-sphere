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

package io.shardingsphere.core.parsing.parser.sql.dml.insert;

import com.google.common.base.Optional;
import io.shardingsphere.core.metadata.table.ShardingTableMetaData;
import io.shardingsphere.core.parsing.lexer.LexerEngine;
import io.shardingsphere.core.parsing.lexer.token.DefaultKeyword;
import io.shardingsphere.core.parsing.lexer.token.Symbol;
import io.shardingsphere.core.parsing.parser.clause.facade.AbstractInsertClauseParserFacade;
import io.shardingsphere.core.parsing.parser.context.condition.Column;
import io.shardingsphere.core.parsing.parser.sql.SQLParser;
import io.shardingsphere.core.parsing.parser.sql.dml.DMLStatement;
import io.shardingsphere.core.parsing.parser.token.ItemsToken;
import io.shardingsphere.core.rule.ShardingRule;
import lombok.AccessLevel;
import lombok.Getter;

/**
 * Insert parser.
 *
 * @author zhangliang
 * @author panjuan
 * @author maxiaoguang
 */
public abstract class AbstractInsertParser implements SQLParser {
    /**
     * 分片规则
     */
    @Getter(AccessLevel.PROTECTED)
    private final ShardingRule shardingRule;
    /**
     * 分表元数据
     */
    @Getter(AccessLevel.PROTECTED)
    private final ShardingTableMetaData shardingTableMetaData;
    /**
     * 词法解析引擎
     */
    @Getter(AccessLevel.PROTECTED)
    private final LexerEngine lexerEngine;

    /**
     * insert语句解析实现
     */
    private final AbstractInsertClauseParserFacade insertClauseParserFacade;
    
    public AbstractInsertParser(
            final ShardingRule shardingRule, final ShardingTableMetaData shardingTableMetaData, final LexerEngine lexerEngine, final AbstractInsertClauseParserFacade insertClauseParserFacade) {
        this.shardingRule = shardingRule;
        this.shardingTableMetaData = shardingTableMetaData;
        this.lexerEngine = lexerEngine;
        this.insertClauseParserFacade = insertClauseParserFacade;
    }

    /**
     * DML语句解析包括 insert delete update等数据操纵语言
     * @return
     */
    @Override
    public final DMLStatement parse() {
        /** 词法解析 */
        lexerEngine.nextToken();
        InsertStatement result = new InsertStatement();
        /** 解析insert into */
        insertClauseParserFacade.getInsertIntoClauseParser().parse(result);
        /** Parse insert columns */
        insertClauseParserFacade.getInsertColumnsClauseParser().parse(result, shardingTableMetaData);
        /** 如果解析到select 或 ( */
        if (lexerEngine.equalAny(DefaultKeyword.SELECT, Symbol.LEFT_PAREN)) {
            throw new UnsupportedOperationException("Cannot INSERT SELECT");
        }
        /** 解析values */
        insertClauseParserFacade.getInsertValuesClauseParser().parse(result);
        /** 解析insert set
         * INSERT INTO test SET id = 4  ON DUPLICATE KEY UPDATE name = 'doubi', name = 'hehe';
         * INSERT INTO test SET id = 4, name = 'hehe'; */
        insertClauseParserFacade.getInsertSetClauseParser().parse(result);
        /** 解析多个key */
        insertClauseParserFacade.getInsertDuplicateKeyUpdateClauseParser().parse(result);
        /** 处理自增主键 */
        processGeneratedKey(result);
        return result;
    }

    /**
     * 当表设置自动生成键，并且插入SQL没写自增字段，增加该字段。
     * 主键为user_id
     * INSERT INTO t_user(nickname, age) VALUES (?, ?)
     * @param insertStatement
     */
    private void processGeneratedKey(final InsertStatement insertStatement) {
        // 当表设置自动生成键，并且插入SQL没写自增字段
        String tableName = insertStatement.getTables().getSingleTableName();
        // 从配置中获取自增主键
        Optional<Column> generateKeyColumn = shardingRule.getGenerateKeyColumn(tableName);
        /** insert 语句中是否有自增主键字段 */
        if (-1 != insertStatement.getGenerateKeyColumnIndex() || !generateKeyColumn.isPresent()) {
            return;
        }
        /** 如果没有则加入insert 语句中 */
        if (DefaultKeyword.VALUES.equals(insertStatement.getInsertValues().getInsertValues().get(0).getType())) {
            if (!insertStatement.getItemsTokens().isEmpty()) {
                insertStatement.getItemsTokens().get(0).getItems().add(generateKeyColumn.get().getName());
            } else {
                ItemsToken columnsToken = new ItemsToken(insertStatement.getColumnsListLastPosition());
                columnsToken.getItems().add(generateKeyColumn.get().getName());
                insertStatement.addSQLToken(columnsToken);
            }
        }
    }
}
