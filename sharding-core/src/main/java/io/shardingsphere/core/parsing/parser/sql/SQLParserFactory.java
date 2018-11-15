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

package io.shardingsphere.core.parsing.parser.sql;

import io.shardingsphere.core.constant.DatabaseType;
import io.shardingsphere.core.metadata.table.ShardingTableMetaData;
import io.shardingsphere.core.parsing.lexer.LexerEngine;
import io.shardingsphere.core.parsing.lexer.dialect.mysql.MySQLKeyword;
import io.shardingsphere.core.parsing.lexer.token.DefaultKeyword;
import io.shardingsphere.core.parsing.lexer.token.Keyword;
import io.shardingsphere.core.parsing.lexer.token.TokenType;
import io.shardingsphere.core.parsing.parser.exception.SQLParsingUnsupportedException;
import io.shardingsphere.core.parsing.parser.sql.dal.DALStatement;
import io.shardingsphere.core.parsing.parser.sql.dal.describe.DescribeParserFactory;
import io.shardingsphere.core.parsing.parser.sql.dal.show.ShowParserFactory;
import io.shardingsphere.core.parsing.parser.sql.dal.use.UseParserFactory;
import io.shardingsphere.core.parsing.parser.sql.dcl.DCLStatement;
import io.shardingsphere.core.parsing.parser.sql.dcl.alter.AlterUserParserFactory;
import io.shardingsphere.core.parsing.parser.sql.dcl.create.CreateUserParserFactory;
import io.shardingsphere.core.parsing.parser.sql.dcl.deny.DenyUserParserFactory;
import io.shardingsphere.core.parsing.parser.sql.dcl.drop.DropUserParserFactory;
import io.shardingsphere.core.parsing.parser.sql.dcl.grant.GrantUserParserFactory;
import io.shardingsphere.core.parsing.parser.sql.dcl.rename.RenameUserParserFactory;
import io.shardingsphere.core.parsing.parser.sql.dcl.revoke.RevokeUserParserFactory;
import io.shardingsphere.core.parsing.parser.sql.ddl.DDLStatement;
import io.shardingsphere.core.parsing.parser.sql.ddl.alter.table.AlterTableParserFactory;
import io.shardingsphere.core.parsing.parser.sql.ddl.create.index.CreateIndexParserFactory;
import io.shardingsphere.core.parsing.parser.sql.ddl.create.table.CreateTableParserFactory;
import io.shardingsphere.core.parsing.parser.sql.ddl.drop.index.DropIndexParserFactory;
import io.shardingsphere.core.parsing.parser.sql.ddl.drop.table.DropTableParserFactory;
import io.shardingsphere.core.parsing.parser.sql.ddl.truncate.table.TruncateTableParserFactory;
import io.shardingsphere.core.parsing.parser.sql.dml.DMLStatement;
import io.shardingsphere.core.parsing.parser.sql.dml.delete.DeleteParserFactory;
import io.shardingsphere.core.parsing.parser.sql.dml.insert.InsertParserFactory;
import io.shardingsphere.core.parsing.parser.sql.dml.update.UpdateParserFactory;
import io.shardingsphere.core.parsing.parser.sql.dql.DQLStatement;
import io.shardingsphere.core.parsing.parser.sql.dql.select.SelectParserFactory;
import io.shardingsphere.core.parsing.parser.sql.tcl.TCLParserFactory;
import io.shardingsphere.core.parsing.parser.sql.tcl.TCLStatement;
import io.shardingsphere.core.rule.ShardingRule;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * SQL parser factory.
 * SQL解析器工厂
 * @author zhangliang
 * @author panjuan
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class SQLParserFactory {
    
    /**
     * Create SQL parser.
     * 根据类型创建不同SQL类型的解析器
     * @param dbType database type
     * @param tokenType token type
     * @param shardingRule databases and tables sharding rule
     * @param lexerEngine lexical analysis engine
     * @param shardingTableMetaData sharding metadata
     * @return SQL parser
     */
    public static SQLParser newInstance(
            final DatabaseType dbType, final TokenType tokenType, final ShardingRule shardingRule, final LexerEngine lexerEngine, final ShardingTableMetaData shardingTableMetaData) {
        // 查询语句SQL解析器
        if (DQLStatement.isDQL(tokenType)) {
            return getDQLParser(dbType, shardingRule, lexerEngine, shardingTableMetaData);
        }// DML 语句解析器
        if (DMLStatement.isDML(tokenType)) {
            return getDMLParser(dbType, tokenType, shardingRule, lexerEngine, shardingTableMetaData);
        }// TCL 语句解析器
        if (TCLStatement.isTCL(tokenType)) {
            return getTCLParser(dbType, shardingRule, lexerEngine);
        }// DAL 语句解析器
        if (DALStatement.isDAL(tokenType)) {
            return getDALParser(dbType, (Keyword) tokenType, shardingRule, lexerEngine);
        }
        /** 获取下一个分词 */
        lexerEngine.nextToken();
        /** 获取下一个分词的token类型 */
        TokenType secondaryTokenType = lexerEngine.getCurrentToken().getType();
        /** 根据当前分词类型和下一个分词类型进行判断*/
        // DDL 语句解析器
        if (DDLStatement.isDDL(tokenType, secondaryTokenType)) {
            return getDDLParser(dbType, tokenType, shardingRule, lexerEngine);
        }// DCL 语句解析器
        if (DCLStatement.isDCL(tokenType, secondaryTokenType)) {
            return getDCLParser(dbType, tokenType, shardingRule, lexerEngine);
        }
        // 抛出不支持SQL语句异常
        throw new SQLParsingUnsupportedException(tokenType);
    }

    /**
     * DQL 语句解析器（不同数据库不一样）
     * @param dbType
     * @param shardingRule
     * @param lexerEngine
     * @param shardingTableMetaData
     * @return
     */
    private static SQLParser getDQLParser(final DatabaseType dbType, final ShardingRule shardingRule, final LexerEngine lexerEngine, final ShardingTableMetaData shardingTableMetaData) {
        /** 创建DQL 语句解析器*/
        return SelectParserFactory.newInstance(dbType, shardingRule, lexerEngine, shardingTableMetaData);
    }
    
    private static SQLParser getDMLParser(
            final DatabaseType dbType, final TokenType tokenType, final ShardingRule shardingRule, final LexerEngine lexerEngine, final ShardingTableMetaData shardingTableMetaData) {
        switch ((DefaultKeyword) tokenType) {
            case INSERT:
                return InsertParserFactory.newInstance(dbType, shardingRule, lexerEngine, shardingTableMetaData);
            case UPDATE:
                return UpdateParserFactory.newInstance(dbType, shardingRule, lexerEngine);
            case DELETE:
                return DeleteParserFactory.newInstance(dbType, shardingRule, lexerEngine);
            default:
                throw new SQLParsingUnsupportedException(tokenType);
        }
    }
    
    private static SQLParser getDDLParser(final DatabaseType dbType, final TokenType tokenType, final ShardingRule shardingRule, final LexerEngine lexerEngine) {
        lexerEngine.skipUntil(DefaultKeyword.INDEX, DefaultKeyword.TABLE);
        if (lexerEngine.isEnd()) {
            throw new SQLParsingUnsupportedException(tokenType);
        }
        return DefaultKeyword.TABLE == lexerEngine.getCurrentToken().getType() ? getTableDDLParser(dbType, tokenType, shardingRule, lexerEngine)
                : getIndexDDLParser(dbType, tokenType, shardingRule, lexerEngine);
    }
    
    private static SQLParser getTableDDLParser(final DatabaseType dbType, final TokenType tokenType, final ShardingRule shardingRule, final LexerEngine lexerEngine) {
        switch ((DefaultKeyword) tokenType) {
            case CREATE:
                return CreateTableParserFactory.newInstance(dbType, shardingRule, lexerEngine);
            case ALTER:
                return AlterTableParserFactory.newInstance(dbType, shardingRule, lexerEngine);
            case DROP:
                return DropTableParserFactory.newInstance(dbType, shardingRule, lexerEngine);
            case TRUNCATE:
                return TruncateTableParserFactory.newInstance(dbType, shardingRule, lexerEngine);
            default:
                throw new SQLParsingUnsupportedException(tokenType);
        }
    }
    
    private static SQLParser getIndexDDLParser(final DatabaseType dbType, final TokenType tokenType, final ShardingRule shardingRule, final LexerEngine lexerEngine) {
        switch ((DefaultKeyword) tokenType) {
            case CREATE:
                return CreateIndexParserFactory.newInstance(dbType, shardingRule, lexerEngine);
            case DROP:
                return DropIndexParserFactory.newInstance(dbType, shardingRule, lexerEngine);
            default:
                throw new SQLParsingUnsupportedException(tokenType);
        }
    }
    
    private static SQLParser getTCLParser(final DatabaseType dbType, final ShardingRule shardingRule, final LexerEngine lexerEngine) {
        return TCLParserFactory.newInstance(dbType, shardingRule, lexerEngine);
    }
    
    private static SQLParser getDALParser(final DatabaseType dbType, final Keyword tokenType, final ShardingRule shardingRule, final LexerEngine lexerEngine) {
        if (DefaultKeyword.USE == tokenType) {
            return UseParserFactory.newInstance(dbType, shardingRule, lexerEngine);
        }
        if (DefaultKeyword.DESC == tokenType || MySQLKeyword.DESCRIBE == tokenType) {
            return DescribeParserFactory.newInstance(dbType, shardingRule, lexerEngine);
        }
        if (MySQLKeyword.SHOW == tokenType) {
            return ShowParserFactory.newInstance(dbType, shardingRule, lexerEngine);
        }
        throw new SQLParsingUnsupportedException(tokenType);
    }

    /**
     * 创建DCL SQL解析器
     * @param dbType
     * @param tokenType
     * @param shardingRule
     * @param lexerEngine
     * @return
     */
    private static SQLParser getDCLParser(final DatabaseType dbType, final TokenType tokenType, final ShardingRule shardingRule, final LexerEngine lexerEngine) {
        switch ((DefaultKeyword) tokenType) {
            case CREATE:
                return CreateUserParserFactory.newInstance(dbType, shardingRule, lexerEngine);
            case ALTER:
                return AlterUserParserFactory.newInstance(dbType, shardingRule, lexerEngine);
            case DROP:
                return DropUserParserFactory.newInstance(dbType, shardingRule, lexerEngine);
            case RENAME:
                return RenameUserParserFactory.newInstance(dbType, shardingRule, lexerEngine);
            case GRANT:
                return GrantUserParserFactory.newInstance(dbType, shardingRule, lexerEngine);
            case REVOKE:
                return RevokeUserParserFactory.newInstance(dbType, shardingRule, lexerEngine);
            case DENY:
                return DenyUserParserFactory.newInstance(dbType, shardingRule, lexerEngine);
            default:
                throw new SQLParsingUnsupportedException(tokenType);
        }
    }
}
