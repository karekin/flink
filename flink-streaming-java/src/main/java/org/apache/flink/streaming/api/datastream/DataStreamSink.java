/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.datastream;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.operators.SlotSharingGroup;
import org.apache.flink.api.common.operators.util.OperatorValidationUtils;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.api.transformations.LegacySinkTransformation;
import org.apache.flink.streaming.api.transformations.PhysicalTransformation;
import org.apache.flink.streaming.api.transformations.SinkTransformation;
import org.apache.flink.streaming.api.transformations.SinkV1Adapter;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A Stream Sink. This is used for emitting elements from a streaming topology.
 *
 * @param <T> The type of the elements in the Stream
 *
 * <p>该类代表一个数据流的 Sink，用于在流式拓扑中发送（输出）元素。可以通过该类将处理好的数据写出到外部系统或者下游算子。
 */
@Public
public class DataStreamSink<T> {

    // 物理转换对象，代表了实际的 Sink 转换操作
    private final PhysicalTransformation<T> transformation;

    /**
     * 构造方法，使用指定的物理转换操作来初始化 DataStreamSink。
     *
     * @param transformation 物理转换操作，不能为 null
     */
    protected DataStreamSink(PhysicalTransformation<T> transformation) {
        this.transformation = checkNotNull(transformation);
    }

    /**
     * @授课老师(V): yi_locus
     * email: 156184212@qq.com
     * 构建DataStreamSink
     * @param inputStream 上一个DataStream，用于提供数据
     * @param sinkFunction 用户自定义的 SinkFunction，用于处理或输出数据
     * @param <T> 数据类型
     * @return 创建好的 DataStreamSink 实例
     *
     * <p>该静态方法会将用户传入的 SinkFunction 包装成一个 StreamSink，然后构建一个 LegacySinkTransformation，
     * 最后将该转换加入到执行环境中，以此完成一个可用的输出算子。
     */
    static <T> DataStreamSink<T> forSinkFunction(
            DataStream<T> inputStream, SinkFunction<T> sinkFunction) {
        // 基于用户的 SinkFunction 创建 StreamSink
        StreamSink<T> sinkOperator = new StreamSink<>(sinkFunction);
        final StreamExecutionEnvironment executionEnvironment =
                inputStream.getExecutionEnvironment();
        /**
         * 构建 LegacySinkTransformation 输出转换
         */
        PhysicalTransformation<T> transformation =
                new LegacySinkTransformation<>(
                        inputStream.getTransformation(),
                        "Unnamed",
                        sinkOperator,
                        executionEnvironment.getParallelism(),
                        false);
        /**
         * 将当前转换加到执行环境的 Transformation 列表中
         */
        executionEnvironment.addOperator(transformation);
        /**
         * 返回 DataStreamSink 实例
         */
        return new DataStreamSink<>(transformation);
    }

    /**
     * 内部使用的静态方法，用于基于新的 Sink API（org.apache.flink.api.connector.sink.Sink）创建 DataStreamSink。
     *
     * @param inputStream 上游 DataStream
     * @param sink 目标 Sink 对象
     * @param customSinkOperatorUidHashes 自定义的算子 UID 哈希信息
     * @param <T> 数据类型
     * @return 创建好的 DataStreamSink 实例
     */
    @Internal
    public static <T> DataStreamSink<T> forSink(
            DataStream<T> inputStream,
            Sink<T> sink,
            CustomSinkOperatorUidHashes customSinkOperatorUidHashes) {
        final StreamExecutionEnvironment executionEnvironment =
                inputStream.getExecutionEnvironment();
        // 使用 SinkTransformation 来封装新的 Sink
        SinkTransformation<T, T> transformation =
                new SinkTransformation<>(
                        inputStream,
                        sink,
                        inputStream.getType(),
                        "Sink",
                        executionEnvironment.getParallelism(),
                        false,
                        customSinkOperatorUidHashes);
        // 将此转换加到执行环境中
        executionEnvironment.addOperator(transformation);
        return new DataStreamSink<>(transformation);
    }

    /**
     * 内部使用的静态方法，用于将老版本的 org.apache.flink.api.connector.sink.Sink 接口封装成新的 SinkV1Adapter，
     * 进而复用 forSink 方法完成创建。
     *
     * @param inputStream 上游 DataStream
     * @param sink 旧版的 Sink 对象
     * @param customSinkOperatorUidHashes 自定义算子 UID 哈希信息
     * @param <T> 数据类型
     * @return 创建好的 DataStreamSink 实例
     */
    @Internal
    public static <T> DataStreamSink<T> forSinkV1(
            DataStream<T> inputStream,
            org.apache.flink.api.connector.sink.Sink<T, ?, ?, ?> sink,
            CustomSinkOperatorUidHashes customSinkOperatorUidHashes) {
        return forSink(inputStream, SinkV1Adapter.wrap(sink), customSinkOperatorUidHashes);
    }

    /**
     * 获取该 DataStreamSink 实际使用的 Transformation（转换操作），其中包含真正的 Sink operator。
     *
     * @return 返回此 Sink 所使用的 Transformation。
     */
    @Internal
    public Transformation<T> getTransformation() {
        return transformation;
    }

    /**
     * 获取 LegacySinkTransformation 转换。如果当前 transformation 不是 LegacySinkTransformation 类型，则会抛出异常。
     *
     * @return 如果 transformation 为 LegacySinkTransformation，则返回该对象
     * @throws IllegalStateException 当 transformation 不是 LegacySinkTransformation 时抛出
     */
    @Internal
    public LegacySinkTransformation<T> getLegacyTransformation() {
        if (transformation instanceof LegacySinkTransformation) {
            return (LegacySinkTransformation<T>) transformation;
        } else {
            throw new IllegalStateException("There is no the LegacySinkTransformation.");
        }
    }

    /**
     * 设置此 Sink 的名称。该名称会在可视化和运行时日志中使用。
     *
     * @param name 要设置的名称
     * @return 返回当前 DataStreamSink 实例，便于链式调用
     */
    public DataStreamSink<T> name(String name) {
        transformation.setName(name);
        return this;
    }

    /**
     * 为此算子设置一个唯一的 ID。
     *
     * <p>指定的 ID 在作业提交时可用于保留相同的算子 ID（例如从 savepoint 重启作业）。
     *
     * <p><strong>重要：</strong>在一个作业中，每个 Transformation 的 ID 必须唯一，否则作业提交将失败。
     *
     * @param uid 用户指定的唯一 ID
     * @return 返回当前 DataStreamSink 实例，便于链式调用
     */
    @PublicEvolving
    public DataStreamSink<T> uid(String uid) {
        transformation.setUid(uid);
        return this;
    }

    /**
     * 为此算子设置一个用户提供的哈希，用于创建 JobVertexID。
     *
     * <p>用户提供的哈希可以用来替代自动生成的哈希，在默认哈希失效的情况下（例如不同版本的 Flink 之间的作业迁移）保持状态映射。
     *
     * <p><strong>重要：</strong>每个 Transformation 的自定义哈希也必须唯一，否则作业提交将失败。同时，此方法不能在
     * operator chain 中的中间节点上使用，否则作业也会失败。
     *
     * @param uidHash 用户提供的哈希字符串，将直接用作 JobVertexID
     * @return 返回当前 DataStreamSink 实例，便于链式调用
     * @throws UnsupportedOperationException 当非 LegacySinkTransformation 尝试调用该方法时抛出异常
     */
    @PublicEvolving
    public DataStreamSink<T> setUidHash(String uidHash) {
        if (!(transformation instanceof LegacySinkTransformation)) {
            throw new UnsupportedOperationException(
                    "Cannot set a custom UID hash on a non-legacy sink");
        }
        transformation.setUidHash(uidHash);
        return this;
    }

    /**
     * 设置此 Sink 的并行度。并行度必须大于零。
     *
     * @param parallelism 要设置的并行度
     * @return 返回当前 DataStreamSink 实例，便于链式调用
     */
    public DataStreamSink<T> setParallelism(int parallelism) {
        transformation.setParallelism(parallelism);
        return this;
    }

    /**
     * 设置此 Sink 的最大并行度（MaxParallelism）。
     *
     * <p>最大并行度指定动态扩缩容的上限。该值必须大于零并小于上限值（Flink 中一般为 2^15）。
     *
     * @param maxParallelism 要设置的最大并行度
     * @return 返回当前 DataStreamSink 实例，便于链式调用
     */
    public DataStreamSink<T> setMaxParallelism(int maxParallelism) {
        OperatorValidationUtils.validateMaxParallelism(maxParallelism, true);
        transformation.setMaxParallelism(maxParallelism);
        return this;
    }

    /**
     * 设置此 Sink 的描述信息。
     *
     * <p>描述信息会显示在 JSON plan 和 web UI 中，而在日志和指标中只会显示名称（name）。
     * 因此，建议在描述信息中提供更详细的内容，而在名称中只提供简要的概览。
     *
     * @param description 为此 Sink 设置的描述信息
     * @return 返回当前 DataStreamSink 实例，便于链式调用
     */
    @PublicEvolving
    public DataStreamSink<T> setDescription(String description) {
        transformation.setDescription(description);
        return this;
    }

    // 下面的资源相关配置是实验性或尚未完成的特性，因此暂时将 setter 方法设为 private

    /**
     * 设置最小和首选资源配置。未来会在资源自动伸缩特性中考虑这个区间。
     *
     * @param minResources 最小资源配置
     * @param preferredResources 首选资源配置
     * @return 当前 DataStreamSink 实例
     */
    private DataStreamSink<T> setResources(
            ResourceSpec minResources, ResourceSpec preferredResources) {
        transformation.setResources(minResources, preferredResources);

        return this;
    }

    /**
     * 设置资源配置，最小和首选资源相同。
     *
     * @param resources 资源配置
     * @return 当前 DataStreamSink 实例
     */
    private DataStreamSink<T> setResources(ResourceSpec resources) {
        transformation.setResources(resources, resources);

        return this;
    }

    /**
     * 关闭此算子的 chaining。这样做会禁止使用线程共驻（co-location）进行优化。
     *
     * <p>可以通过 {@link org.apache.flink.streaming.api.environment.StreamExecutionEnvironment#disableOperatorChaining()}
     * 关闭整个作业的 chaining，但会影响性能，因此一般不建议这样做。
     *
     * @return 返回当前 DataStreamSink 实例，便于链式调用
     */
    @PublicEvolving
    public DataStreamSink<T> disableChaining() {
        this.transformation.setChainingStrategy(ChainingStrategy.NEVER);
        return this;
    }

    /**
     * 为该算子设置 slot sharing group。相同 slot sharing group 中的并行实例会尽可能在同一个 TaskManager slot 中执行。
     *
     * <p>如果所有输入算子都在同一个 slot sharing group 且未显式指定分组，则会继承输入算子的分组。
     *
     * <p>操作初始时位于默认的 slot sharing group。可以将算子显式地设置到名称为 {@code "default"} 的分组中。
     *
     * @param slotSharingGroup 要设置的 slot sharing group 名称
     * @return 当前 DataStreamSink 实例，便于链式调用
     */
    @PublicEvolving
    public DataStreamSink<T> slotSharingGroup(String slotSharingGroup) {
        transformation.setSlotSharingGroup(slotSharingGroup);
        return this;
    }

    /**
     * 为该算子设置 slot sharing group。相同 slot sharing group 中的并行实例会尽可能在同一个 TaskManager slot 中执行。
     *
     * <p>如果所有输入算子都在同一个 slot sharing group 且未显式指定分组，则会继承输入算子的分组。
     *
     * <p>操作初始时位于默认的 slot sharing group。可以将算子显式地设置到名称为 {@code "default"} 的分组中。
     *
     * @param slotSharingGroup slot sharing group 对象，包含组名以及相关的资源配置
     * @return 当前 DataStreamSink 实例，便于链式调用
     */
    @PublicEvolving
    public DataStreamSink<T> slotSharingGroup(SlotSharingGroup slotSharingGroup) {
        transformation.setSlotSharingGroup(slotSharingGroup);
        return this;
    }
}

