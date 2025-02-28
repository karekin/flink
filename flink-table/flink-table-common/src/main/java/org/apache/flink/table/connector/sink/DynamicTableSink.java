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

package org.apache.flink.table.connector.sink;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.RuntimeConverter;
import org.apache.flink.table.connector.sink.abilities.SupportsOverwrite;
import org.apache.flink.table.connector.sink.abilities.SupportsPartitioning;
import org.apache.flink.table.connector.sink.abilities.SupportsWritingMetadata;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Optional;

/**
 * 动态表（Dynamic Table）的外部存储系统的 Sink。
 *
 * <p>动态表是 Flink Table & SQL API 的核心概念之一，用于统一处理有界（bounded）和无界（unbounded）数据。
 * 动态表可以随着时间的推移发生变化。
 *
 * <p>当写入动态表时，其内容可以被视为一个变更日志（changelog），该日志可能是有限的也可能是无限的，
 * 并且所有更改都会被持续写入，直到变更日志耗尽。
 * 指定的 {@link ChangelogMode} 表示 Sink 在运行时接受的变更类型。
 *
 * <p>对于常规的批处理（batch）场景，Sink 仅接受插入（insert-only）行，并写入有界流。
 *
 * <p>对于常规的流处理（streaming）场景，Sink 仅接受插入（insert-only）行，并写入无界流。
 *
 * <p>对于变更数据捕获（CDC）场景，Sink 可以写入有界或无界流，并且可以包含插入、更新和删除行，
 * 详情见 {@link RowKind}。
 *
 * <p>{@link DynamicTableSink} 的实例可以被视为工厂，它们最终会创建实际用于写入数据的运行时实现。
 *
 * <p>根据声明的可选能力，优化器可能会对实例进行更改，从而影响生成的运行时实现。
 *
 * <p>{@link DynamicTableSink} 可以实现以下能力：
 * <ul>
 *   <li>{@link SupportsPartitioning} - 支持分区写入</li>
 *   <li>{@link SupportsOverwrite} - 支持数据覆盖</li>
 *   <li>{@link SupportsWritingMetadata} - 支持写入元数据</li>
 * </ul>
 *
 * <p>在最后一步，优化器会调用 {@link #getSinkRuntimeProvider(Context)} 来获取运行时提供程序。
 */
@PublicEvolving
public interface DynamicTableSink {

    /**
     * 返回 Sink 在运行时可以接受的变更集。
     *
     * <p>优化器可以提供建议，但最终由 Sink 决定接受哪些变更。
     * 如果优化器不支持某种模式，则会抛出错误。例如，Sink 可能仅支持 {@link ChangelogMode#insertOnly()}。
     *
     * @param requestedMode 当前计划期望的变更模式
     */
    ChangelogMode getChangelogMode(ChangelogMode requestedMode);

    /**
     * 返回用于写入数据的运行时实现提供程序。
     *
     * <p>由于可能存在不同的运行时接口，因此 {@link SinkRuntimeProvider} 作为基础接口。
     * 具体的 {@link SinkRuntimeProvider} 可能位于其他 Flink 模块中。
     *
     * <p>无论使用哪种提供程序接口，表运行时都期望 Sink 实现能够接受内部数据结构（详见 {@link RowData}）。
     *
     * <p>提供的 {@link Context} 提供了一些实用工具，使得运行时实现的创建可以尽可能减少对内部数据结构的依赖。
     *
     * <p>{@link SinkV2Provider} 是推荐的核心接口。
     * {@link SinkProvider}、{@code SinkFunctionProvider}（位于 `flink-table-api-java-bridge`）
     * 和 {@link OutputFormatProvider} 主要用于向后兼容。
     *
     * @param context 运行时上下文
     * @return 运行时提供程序实例
     * @see SinkV2Provider
     */
    SinkRuntimeProvider getSinkRuntimeProvider(Context context);

    /**
     * 在优化器规划阶段创建此实例的副本。
     * 副本应该是所有可变成员的深拷贝。
     *
     * @return 此 Sink 实例的副本
     */
    DynamicTableSink copy();

    /**
     * 返回一个用于打印到控制台或日志的摘要字符串。
     *
     * @return Sink 的描述信息
     */
    String asSummaryString();

    // --------------------------------------------------------------------------------------------
    // 辅助接口
    // --------------------------------------------------------------------------------------------

    /**
     * 运行时提供程序的创建上下文。
     *
     * <p>它提供了一些实用工具，使得运行时实现的创建可以尽可能减少对内部数据结构的依赖。
     *
     * <p>方法应在 {@link #getSinkRuntimeProvider(Context)} 中调用。
     * 返回的实例应实现 {@link Serializable}，并可直接传递到运行时实现类中。
     */
    @PublicEvolving
    interface Context {

        /**
         * 返回运行时实现是否可以期望行数是有限的。
         *
         * <p>此信息可能来源于会话的执行模式或查询的类型。
         *
         * @return 是否为有界流
         */
        boolean isBounded();

        /**
         * 创建描述内部数据结构的类型信息。
         *
         * @param consumedDataType 被消费的数据类型
         * @return 对应的类型信息
         * @see ResolvedSchema#toPhysicalRowDataType()
         */
        <T> TypeInformation<T> createTypeInformation(DataType consumedDataType);

        /**
         * 创建描述内部数据结构的类型信息（基于 {@link LogicalType}）。
         *
         * @param consumedLogicalType 被消费的逻辑类型
         * @return 对应的类型信息
         */
        <T> TypeInformation<T> createTypeInformation(LogicalType consumedLogicalType);

        /**
         * 创建一个转换器，将 Flink 内部数据结构转换为可传递到运行时实现的对象。
         *
         * <p>例如，{@link RowData} 可以转换为 {@link Row}，
         * 或者结构化类型的内部表示可以转换回原始（可能是嵌套的）POJO。
         *
         * @param consumedDataType 被消费的数据类型
         * @return 数据结构转换器
         * @see LogicalType#supportsOutputConversion(Class)
         */
        DataStructureConverter createDataStructureConverter(DataType consumedDataType);
    }

    /**
     * 数据结构转换器，用于在 Flink 内部数据结构和外部对象之间进行映射。
     *
     * <p>例如，{@link RowData} 可以转换为 {@link Row}，
     * 或者结构化类型的内部表示可以转换回原始（可能是嵌套的）POJO。
     *
     * @see LogicalType#supportsOutputConversion(Class)
     */
    @PublicEvolving
    interface DataStructureConverter extends RuntimeConverter {

        /**
         * 将内部结构转换为外部对象。
         *
         * @param internalStructure 内部数据结构
         * @return 转换后的外部对象
         */
        @Nullable
        Object toExternal(@Nullable Object internalStructure);
    }

    /**
     * 提供实际的运行时实现，用于写入数据。
     *
     * <p>{@link SinkV2Provider} 是推荐的核心接口。
     * {@link SinkProvider}、{@code SinkFunctionProvider}（位于 `flink-table-api-java-bridge`）
     * 和 {@link OutputFormatProvider} 主要用于向后兼容。
     *
     * @see SinkV2Provider
     */
    @PublicEvolving
    interface SinkRuntimeProvider {
        // 标记接口
    }
}

