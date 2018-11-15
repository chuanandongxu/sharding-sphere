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

package io.shardingsphere.core.parsing.parser.sql.dql.select;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import io.shardingsphere.core.constant.AggregationType;
import io.shardingsphere.core.metadata.table.ShardingTableMetaData;
import io.shardingsphere.core.parsing.lexer.LexerEngine;
import io.shardingsphere.core.parsing.lexer.token.Assist;
import io.shardingsphere.core.parsing.lexer.token.DefaultKeyword;
import io.shardingsphere.core.parsing.lexer.token.Symbol;
import io.shardingsphere.core.parsing.parser.clause.facade.AbstractSelectClauseParserFacade;
import io.shardingsphere.core.parsing.parser.constant.DerivedColumn;
import io.shardingsphere.core.parsing.parser.context.OrderItem;
import io.shardingsphere.core.parsing.parser.context.selectitem.AggregationSelectItem;
import io.shardingsphere.core.parsing.parser.context.selectitem.SelectItem;
import io.shardingsphere.core.parsing.parser.context.selectitem.StarSelectItem;
import io.shardingsphere.core.parsing.parser.context.table.Table;
import io.shardingsphere.core.parsing.parser.sql.SQLParser;
import io.shardingsphere.core.parsing.parser.token.ItemsToken;
import io.shardingsphere.core.parsing.parser.token.OrderByToken;
import io.shardingsphere.core.rule.ShardingRule;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.LinkedList;
import java.util.List;

/**
 * Select parser.
 *
 * @author zhangliang
 */
@RequiredArgsConstructor
@Getter(AccessLevel.PROTECTED)
public abstract class AbstractSelectParser implements SQLParser {
    /**
     * 分片规则
     */
    private final ShardingRule shardingRule;
    /**
     * 词法解析引擎
     */
    private final LexerEngine lexerEngine;
    /**
     * select 从句解析
     */
    private final AbstractSelectClauseParserFacade selectClauseParserFacade;

    private final List<SelectItem> items = new LinkedList<>();
    /**
     * 分表元数据
     */
    private final ShardingTableMetaData shardingTableMetaData;

    /**
     * 解析select sql 语句
     *
     * @return
     */
    @Override
    public final SelectStatement parse() {
        SelectStatement result = parseInternal();
        // 是否包含子查询
        if (result.containsSubQuery()) {
            result = result.mergeSubQueryStatement();
        }
        // TODO move to rewrite
        appendDerivedColumns(result);
        appendDerivedOrderBy(result);
        return result;
    }

    /**
     * 解析SQL
     *
     * @return
     */
    private SelectStatement parseInternal() {
        SelectStatement result = new SelectStatement();
        // 解析SQL的过程中使用lexer进行分词
        lexerEngine.nextToken();
        parseInternal(result);
        return result;
    }

    /**
     * 解析SQL，不同数据库有不同的实现
     *
     * @param selectStatement
     */
    protected abstract void parseInternal(SelectStatement selectStatement);

    /**
     * 解析distinct
     */
    protected final void parseDistinct() {
        selectClauseParserFacade.getDistinctClauseParser().parse();
    }

    /**
     * 解析select列表
     *
     * @param selectStatement
     * @param items
     */
    protected final void parseSelectList(final SelectStatement selectStatement, final List<SelectItem> items) {
        selectClauseParserFacade.getSelectListClauseParser().parse(selectStatement, items);
    }

    /**
     * 解析from
     *
     * @param selectStatement
     */
    protected final void parseFrom(final SelectStatement selectStatement) {
        /** 不支持 into */
        lexerEngine.unsupportedIfEqual(DefaultKeyword.INTO);
        /** 如果是from 则跳过 */
        if (lexerEngine.skipIfEqual(DefaultKeyword.FROM)) {
            /** 解析表 */
            parseTable(selectStatement);
        }
    }

    /**
     * 解析表
     *
     * @param selectStatement
     */
    private void parseTable(final SelectStatement selectStatement) {
        /** 跳过左边括号 */
        if (lexerEngine.skipIfEqual(Symbol.LEFT_PAREN)) {
            /** 设置子查询 */
            selectStatement.setSubQueryStatement(parseInternal());
            /** 如果是where则返回 */
            if (lexerEngine.equalAny(DefaultKeyword.WHERE, Assist.END)) {
                return;
            }
        }
        selectClauseParserFacade.getTableReferencesClauseParser().parse(selectStatement, false);
    }

    protected final void parseWhere(final ShardingRule shardingRule, final SelectStatement selectStatement, final List<SelectItem> items) {
        selectClauseParserFacade.getWhereClauseParser().parse(shardingRule, selectStatement, items);
    }

    protected final void parseGroupBy(final SelectStatement selectStatement) {
        selectClauseParserFacade.getGroupByClauseParser().parse(selectStatement);
    }

    protected final void parseHaving() {
        selectClauseParserFacade.getHavingClauseParser().parse();
    }

    /**
     * 解析 order by
     *
     * @param selectStatement
     */
    protected final void parseOrderBy(final SelectStatement selectStatement) {
        selectClauseParserFacade.getOrderByClauseParser().parse(selectStatement);
    }

    /**
     * 解析剩余的表达式 UINON EXCEPTION等
     */
    protected final void parseSelectRest() {
        selectClauseParserFacade.getSelectRestClauseParser().parse();
    }

    private void appendDerivedColumns(final SelectStatement selectStatement) {
        ItemsToken itemsToken = new ItemsToken(selectStatement.getSelectListLastPosition());
        appendAvgDerivedColumns(itemsToken, selectStatement);
        appendDerivedOrderColumns(itemsToken, selectStatement.getOrderByItems(), selectStatement);
        appendDerivedGroupColumns(itemsToken, selectStatement.getGroupByItems(), selectStatement);
        if (!itemsToken.getItems().isEmpty()) {
            selectStatement.addSQLToken(itemsToken);
        }
    }

    /**
     * * 针对 AVG 聚合字段，增加推导字段
     * * AVG 改写成 SUM + COUNT 查询，内存计算出 AVG 结果。
     *
     * @param itemsToken
     * @param selectStatement
     */
    private void appendAvgDerivedColumns(final ItemsToken itemsToken, final SelectStatement selectStatement) {
        int derivedColumnOffset = 0;
        for (SelectItem each : selectStatement.getItems()) {
            if (!(each instanceof AggregationSelectItem) || AggregationType.AVG != ((AggregationSelectItem) each).getType()) {
                continue;
            }
            AggregationSelectItem avgItem = (AggregationSelectItem) each;
            /** count字段 */
            String countAlias = DerivedColumn.AVG_COUNT_ALIAS.getDerivedColumnAlias(derivedColumnOffset);
            AggregationSelectItem countItem = new AggregationSelectItem(AggregationType.COUNT, avgItem.getInnerExpression(), Optional.of(countAlias));
            /** sum 字段 */
            String sumAlias = DerivedColumn.AVG_SUM_ALIAS.getDerivedColumnAlias(derivedColumnOffset);
            AggregationSelectItem sumItem = new AggregationSelectItem(AggregationType.SUM, avgItem.getInnerExpression(), Optional.of(sumAlias));
            /** AggregationSelectItem 设置 */
            avgItem.getDerivedAggregationSelectItems().add(countItem);
            avgItem.getDerivedAggregationSelectItems().add(sumItem);
            // TODO replace avg to constant, avoid calculate useless avg
            // 将AVG列替换成常数，避免数据库再计算无用的AVG函数
            /** ItemToken */
            itemsToken.getItems().add(countItem.getExpression() + " AS " + countAlias + " ");
            itemsToken.getItems().add(sumItem.getExpression() + " AS " + sumAlias + " ");
            derivedColumnOffset++;
        }
    }

    /**
     * * 针对 GROUP BY 或 ORDER BY 字段，增加推导字段
     * * 如果该字段不在查询字段里，需要额外查询该字段，这样才能在内存里 GROUP BY 或 ORDER BY
     * *
     * * @param itemsToken 选择项标记对象
     * * @param orderItems 排序字段
     * * @param aliasPattern 别名模式
     */
    private void appendDerivedOrderColumns(final ItemsToken itemsToken, final List<OrderItem> orderItems, final SelectStatement selectStatement) {
        int derivedColumnOffset = 0;
        for (OrderItem each : orderItems) {
            if (!containsItem(selectStatement, each)) {
                String alias = DerivedColumn.ORDER_BY_ALIAS.getDerivedColumnAlias(derivedColumnOffset++);
                each.setAlias(Optional.of(alias));
                itemsToken.getItems().add(each.getQualifiedName().get() + " AS " + alias + " ");
            }
        }
    }

    private void appendDerivedGroupColumns(final ItemsToken itemsToken, final List<OrderItem> orderItems, final SelectStatement selectStatement) {
        int derivedColumnOffset = 0;
        for (OrderItem each : orderItems) {
            if (!containsItem(selectStatement, each)) {
                String alias = DerivedColumn.GROUP_BY_ALIAS.getDerivedColumnAlias(derivedColumnOffset++);
                each.setAlias(Optional.of(alias));
                itemsToken.getItems().add(each.getQualifiedName().get() + " AS " + alias + " ");
            }
        }
    }
    /**
     * 查询字段是否包含排序字段
     *
     * @param orderItem 排序字段
     * @return 是否
     */
    private boolean containsItem(final SelectStatement selectStatement, final OrderItem orderItem) {
        return orderItem.isIndex() || containsItemInStarSelectItems(selectStatement, orderItem) || containsItemInSelectItems(selectStatement, orderItem);
    }

    /**
     * 是否包含select *
     * @param selectStatement
     * @param orderItem
     * @return
     */
    private boolean containsItemInStarSelectItems(final SelectStatement selectStatement, final OrderItem orderItem) {
        return selectStatement.hasUnqualifiedStarSelectItem()
                || containsItemWithOwnerInStarSelectItems(selectStatement, orderItem) || containsItemWithoutOwnerInStarSelectItems(selectStatement, orderItem);
    }
    /**
     * 是否包含select table.*
     * @param selectStatement
     * @param orderItem
     * @return
     */
    private boolean containsItemWithOwnerInStarSelectItems(final SelectStatement selectStatement, final OrderItem orderItem) {
        return orderItem.getOwner().isPresent() && selectStatement.findStarSelectItem(orderItem.getOwner().get()).isPresent();
    }

    private boolean containsItemWithoutOwnerInStarSelectItems(final SelectStatement selectStatement, final OrderItem orderItem) {
        if (!orderItem.getOwner().isPresent()) {
            for (StarSelectItem each : selectStatement.getQualifiedStarSelectItems()) {
                if (isSameSelectItem(selectStatement, each, orderItem)) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean isSameSelectItem(final SelectStatement selectStatement, final StarSelectItem starSelectItem, final OrderItem orderItem) {
        Preconditions.checkState(starSelectItem.getOwner().isPresent());
        Preconditions.checkState(orderItem.getName().isPresent());
        Optional<Table> table = selectStatement.getTables().find(starSelectItem.getOwner().get());
        return table.isPresent() && shardingTableMetaData.containsColumn(table.get().getName(), orderItem.getName().get());
    }

    /**
     * 是否包含 SelectItem
     * @param selectStatement
     * @param orderItem
     * @return
     */
    private boolean containsItemInSelectItems(final SelectStatement selectStatement, final OrderItem orderItem) {
        for (SelectItem each : selectStatement.getItems()) {
            if (isSameAlias(each, orderItem) || isSameQualifiedName(each, orderItem)) {
                return true;
            }
        }
        return false;
    }

    /**
     * select a,b,c  order by a,e,f 是否相同
     * a
     * @param selectItem
     * @param orderItem
     * @return
     */
    private boolean isSameAlias(final SelectItem selectItem, final OrderItem orderItem) {
        return selectItem.getAlias().isPresent() && orderItem.getAlias().isPresent() && selectItem.getAlias().get().equalsIgnoreCase(orderItem.getAlias().get());
    }

    /**
     * table.a
     * @param selectItem
     * @param orderItem
     * @return
     */
    private boolean isSameQualifiedName(final SelectItem selectItem, final OrderItem orderItem) {
        return !selectItem.getAlias().isPresent() && orderItem.getQualifiedName().isPresent() && selectItem.getExpression().equalsIgnoreCase(orderItem.getQualifiedName().get());
    }


//    当 SQL 有聚合条件而无排序条件，根据聚合条件进行排序。这是数据库自己的执行规则。
//
//    mysql> SELECT order_id FROM t_order GROUP BY order_id;
//        | order_id |
//            +----------+
//            | 1        |
//            | 2        |
//            | 3        |
//            +----------+
//            3 rows in set (0.05 sec)
//
//    mysql> SELECT order_id FROM t_order GROUP BY order_id DESC;
//        | order_id |
//            +----------+
//            | 3        |
//            | 2        |
//            | 1        |
//            +----------+
/**
 * 当无 Order By 条件时，使用 Group By 作为排序条件（数据库本身规则）
 */
    private void appendDerivedOrderBy(final SelectStatement selectStatement) {
        if (!selectStatement.getGroupByItems().isEmpty() && selectStatement.getOrderByItems().isEmpty()) {
            selectStatement.getOrderByItems().addAll(selectStatement.getGroupByItems());
            selectStatement.addSQLToken(new OrderByToken(selectStatement.getGroupByLastPosition()));
        }
    }
}
