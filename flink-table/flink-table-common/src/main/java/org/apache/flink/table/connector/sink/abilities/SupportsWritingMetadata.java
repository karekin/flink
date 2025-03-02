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
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.utils.LogicalTypeCasts;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 用于支持 {@link DynamicTableSink} 写入元数据列的接口。
 *
 * <p>元数据列为表的模式（schema）添加额外的列。表 Sink 需要接受这些元数据列，
 * 将它们追加到消费的行数据末尾，并负责存储它们。这可能包括将元数据列传递给内部使用的格式化组件。
 *
 * <p>SQL 示例：
 *
 * <pre>{@code
 * // 将数据写入对应的元数据键 `timestamp`
 * CREATE TABLE t1 (i INT, s STRING, timestamp TIMESTAMP(3) WITH LOCAL TIME ZONE METADATA, d DOUBLE)
 *
 * // 从 INT 转换数据并写入元数据键 `timestamp`
 * CREATE TABLE t2 (i INT, s STRING, myTimestamp INT METADATA FROM 'timestamp', d DOUBLE)
 *
 * // 由于元数据列是虚拟的，因此不会持久化元数据
 * CREATE TABLE t3 (i INT, s STRING, timestamp TIMESTAMP(3) WITH LOCAL TIME ZONE METADATA VIRTUAL, d DOUBLE)
 * }</pre>
 *
 * <p>默认情况下，如果未实现此接口，上述 SQL 语句会失败，因为表 Sink 不提供 `timestamp` 这个元数据键。
 *
 * <p>如果实现了该接口，{@link #listWritableMetadata()} 方法会列出 Sink 暴露给查询优化器的所有元数据键及其对应的数据类型。
 * 查询优化器会利用这些信息进行校验，并在必要时插入显式类型转换。
 *
 * <p>查询优化器会选择所需的元数据列，并调用 {@link #applyWritableMetadata(List, DataType)}
 * 传递所需的元数据键列表。实现类需要确保在 `applyWritableMetadata` 方法被调用后，
 * 元数据列会按照提供的列表顺序出现在物理行数据的末尾。
 *
 * <p>元数据列的数据类型必须与 {@link #listWritableMetadata()} 中定义的类型匹配。
 * 例如，对于 `t2`，表 Sink 需要接受 `TIMESTAMP` 类型，而不是 `INT`。
 * 查询优化器会在上游操作中进行类型转换：
 *
 * <pre>{@code
 * // 物理输入
 * ROW < i INT, s STRING, d DOUBLE >
 *
 * // `t1` 的最终输入（即消费的数据类型）
 * ROW < i INT, s STRING, d DOUBLE, timestamp TIMESTAMP(3) WITH LOCAL TIME ZONE >
 *
 * // `t2` 的最终输入（即消费的数据类型）
 * ROW < i INT, s STRING, d DOUBLE, myTimestamp TIMESTAMP(3) WITH LOCAL TIME ZONE >
 *
 * // `t3` 的最终输入（即消费的数据类型）
 * ROW < i INT, s STRING, d DOUBLE >
 * }</pre>
 */
@PublicEvolving
public interface SupportsWritingMetadata {

    /**
     * 返回表 Sink 可以消费并写入的元数据键及其对应的数据类型。
     *
     * <p>查询优化器会使用返回的映射来进行校验，并在必要时插入显式类型转换（参考
     * {@link LogicalTypeCasts#supportsExplicitCast(LogicalType, LogicalType)}）。
     *
     * <p>返回的映射的迭代顺序决定了在 {@link #applyWritableMetadata(List, DataType)} 方法中，
     * 传递的元数据键列表的顺序。因此，如果需要严格控制元数据列的顺序，建议返回 {@link LinkedHashMap}。
     *
     * <p>如果 Sink 需要将元数据传递给一个或多个格式化组件，建议使用以下列顺序：
     *
     * <pre>{@code
     * 键格式的元数据列 + 值格式的元数据列 + Sink 自己的元数据列
     * }</pre>
     *
     * <p>元数据键名的命名方式遵循 {@link Factory} 中描述的模式。
     * 如果格式组件和 Sink 都有相同名称的元数据键，格式组件的键优先级更高。
     *
     * <p>无论返回的 {@link DataType} 是什么，元数据列都会被表示为内部数据结构（参考 {@link RowData}）。
     *
     * @return 元数据键与数据类型的映射
     * @see EncodingFormat#listWritableMetadata()
     */
    Map<String, DataType> listWritableMetadata();

    /**
     * 指定 Sink 需要写入的元数据键列表，并定义消费数据的最终数据类型。
     *
     * <p>查询优化器会调用此方法，提供一个元数据键列表。实现类必须确保这些元数据列
     * 会按照提供的顺序追加到物理行数据的末尾，并且在存储时保持该顺序。
     *
     * @param metadataKeys 需要写入的元数据键列表，该列表是 {@link #listWritableMetadata()} 返回映射的子集，
     *                     并按照返回映射的迭代顺序排列
     * @param consumedDataType Sink 的最终输入数据类型，该类型仅用于传递，查询优化器会决定字段名称以避免冲突
     * @see EncodingFormat#applyWritableMetadata(List)
     */
    void applyWritableMetadata(List<String> metadataKeys, DataType consumedDataType);
}

