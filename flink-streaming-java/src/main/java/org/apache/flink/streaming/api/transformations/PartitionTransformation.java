/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.transformations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;

import org.apache.flink.shaded.guava31.com.google.common.collect.Lists;

import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * 该转换类表示输入元素的分区方式的改变。
 *
 * <p>该转换不会创建物理操作，而仅影响上游操作如何与下游操作连接。
 *
 * <p>它主要用于指定数据流的分区策略，使得数据在计算任务间能够合理地分发，以优化计算性能和数据均衡。
 *
 * @param <T> 该 {@link PartitionTransformation} 处理的元素类型
 */
@Internal
public class PartitionTransformation<T> extends Transformation<T> {

    // 该分区转换所基于的输入转换
    private final Transformation<T> input;

    // 用于决定数据如何在下游任务之间分区的分区器
    private final StreamPartitioner<T> partitioner;

    // 指定数据交换模式，例如管道（Pipelined）或批量（Batch）
    private final StreamExchangeMode exchangeMode;

    /**
     * 使用给定的输入转换和 {@link StreamPartitioner} 创建新的 {@link PartitionTransformation}。
     *
     * @param input 输入的 {@link Transformation}，表示上游的数据转换
     * @param partitioner 负责分区数据的 {@link StreamPartitioner}，用于定义数据如何被分区
     */
    public PartitionTransformation(Transformation<T> input, StreamPartitioner<T> partitioner) {
        this(input, partitioner, StreamExchangeMode.UNDEFINED);
    }

    /**
     * 使用给定的输入转换、{@link StreamPartitioner} 和 {@link StreamExchangeMode} 创建新的 {@link PartitionTransformation}。
     *
     * @param input 输入的 {@link Transformation}，表示上游的数据转换
     * @param partitioner 负责分区数据的 {@link StreamPartitioner}，用于定义数据如何被分区
     * @param exchangeMode 数据交换模式，定义了数据在任务间如何传输（如 Pipelined 或 Batch 模式）
     */
    public PartitionTransformation(
            Transformation<T> input,
            StreamPartitioner<T> partitioner,
            StreamExchangeMode exchangeMode) {
        // 调用父类构造方法，指定名称为 "Partition"，输出类型与输入相同，并行度与输入相同
        super("Partition", input.getOutputType(), input.getParallelism());
        this.input = input;
        this.partitioner = partitioner;
        this.exchangeMode = checkNotNull(exchangeMode); // 确保交换模式不为空
    }

    /**
     * 获取用于分区输入 {@link Transformation} 数据的 {@link StreamPartitioner}。
     *
     * @return 该转换所使用的 StreamPartitioner
     */
    public StreamPartitioner<T> getPartitioner() {
        return partitioner;
    }

    /**
     * 获取该 {@link PartitionTransformation} 的数据交换模式 {@link StreamExchangeMode}。
     *
     * @return 该转换的 StreamExchangeMode
     */
    public StreamExchangeMode getExchangeMode() {
        return exchangeMode;
    }

    /**
     * 获取所有的前置转换（包括当前转换和它的所有上游转换）。
     *
     * <p>该方法返回一个包含当前转换自身以及所有前置转换的列表，以用于构建执行计划。
     *
     * @return 包含当前转换及所有前置转换的列表
     */
    @Override
    protected List<Transformation<?>> getTransitivePredecessorsInternal() {
        List<Transformation<?>> result = Lists.newArrayList();
        result.add(this); // 先加入自身
        result.addAll(input.getTransitivePredecessors()); // 再加入上游转换的前置转换
        return result;
    }

    /**
     * 获取当前转换的所有输入转换。
     *
     * @return 仅包含输入转换的列表
     */
    @Override
    public List<Transformation<?>> getInputs() {
        return Collections.singletonList(input);
    }
}
