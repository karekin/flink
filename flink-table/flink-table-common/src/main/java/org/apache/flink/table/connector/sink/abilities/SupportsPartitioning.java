/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.connector.sink.abilities;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.connector.sink.DynamicTableSink;

import java.util.Map;

/**
 * 允许在 {@link DynamicTableSink} 中写入分区数据的接口。
 *
 * <p>分区数据将存储在外部系统中，并通过一个或多个基于字符串的分区键进行标识。
 * 单个分区表示为 {@code Map<String, String>}，该映射将每个分区键映射到相应的分区值。
 * 分区键及其顺序由 catalog table 定义。
 *
 * <p>例如，数据可以按 "region"（区域）进行分区，在每个区域内再按 "month"（月份）分区。
 * 分区键的顺序（例如：首先按区域分区，然后按月份分区）由 catalog table 定义。
 * 分区列表示例：
 *
 * <pre>
 *   List(
 *     ['region'='europe', 'month'='2020-01'],
 *     ['region'='europe', 'month'='2020-02'],
 *     ['region'='asia', 'month'='2020-01'],
 *     ['region'='asia', 'month'='2020-02']
 *   )
 * </pre>
 *
 * <p>假设存在如下的分区表：
 *
 * <pre>{@code
 * CREATE TABLE t (a INT, b STRING, c DOUBLE, region STRING, month STRING) PARTITION BY (region, month);
 * }</pre>
 *
 * <p>可以使用 {@code INSERT INTO ... PARTITION} 语法向 <i>静态表分区</i> 插入数据：
 *
 * <pre>{@code
 * INSERT INTO t PARTITION (region='europe', month='2020-01') SELECT a, b, c FROM my_view;
 * }</pre>
 *
 * <p>如果 {@code PARTITION} 子句为所有分区键分配了值，则该操作被视为“插入到静态分区”。
 * 在上例中，查询结果将写入静态分区 {@code region='europe', month='2020-01'}，
 * 该静态分区信息会由查询优化器传递给 {@link #applyStaticPartition(Map)} 方法。
 * 另外，查询优化器还可以从查询中的字面量推导出静态分区：
 *
 * <pre>{@code
 * INSERT INTO t SELECT a, b, c, 'asia' AS region, '2020-01' AS month FROM my_view;
 * }</pre>
 *
 * <p>此外，还可以使用 SQL 语法向 <i>动态表分区</i> 插入数据：
 *
 * <pre>{@code
 * INSERT INTO t PARTITION (region='europe') SELECT a, b, c, month FROM another_view;
 * }</pre>
 *
 * <p>如果在 {@code PARTITION} 子句中只有部分分区键具有静态值，或者在 {@code SELECT} 子句中只提供了部分常量值，
 * 该操作被视为“插入到动态分区”。在上例中，静态分区部分为 {@code region='europe'}，
 * 该信息会传递给 {@link #applyStaticPartition(Map)} 方法，而其他分区键的值（例如 {@code month}）
 * 需要在运行时由 sink 处理每条记录时动态获取。
 *
 * <p>如果 {@code PARTITION} 子句没有任何静态值，或者该子句完全省略，则所有分区键的值都需要
 * 通过查询中的静态部分推导，或在运行时动态获取。
 */
@PublicEvolving
public interface SupportsPartitioning {

    /**
     * 提供分区的静态部分。
     *
     * <p>单个分区将每个分区键映射到一个分区值。根据用户定义的语句，分区可能不会包含所有分区键。
     *
     * <p>更多信息请参考 {@link SupportsPartitioning} 的文档。
     *
     * @param partition 用户定义的（可能是不完整的）静态分区信息
     */
    void applyStaticPartition(Map<String, String> partition);

    /**
     * 返回数据在被 sink 消费之前是否需要按分区进行分组。默认情况下，运行时不需要此功能，记录会按照任意分区顺序到达。
     *
     * <p>如果该方法返回 true，则 sink 期望所有记录在消费前已按分区键分组。
     * 换句话说，sink 将会先接收某个分区的所有元素，然后再接收另一个分区的所有元素。
     * 来自不同分区的元素不会混合。这种机制可以用于某些 sink，减少分区写入器的数量，
     * 通过一次写入一个分区来提高写入性能。
     *
     * <p>该方法的参数指示当前执行模式是否支持数据分组。例如，根据执行模式的不同，
     * 在运行时可能无法进行排序操作。
     *
     * @param supportsGrouping 当前执行模式是否支持数据分组
     * @return 是否需要在 sink 消费前按分区进行分组。
     *         如果 {@code supportsGrouping} 为 false，则此方法不应返回 true，否则查询优化器将会失败。
     */
    @SuppressWarnings("unused")
    default boolean requiresPartitionGrouping(boolean supportsGrouping) {
        return false;
    }
}

