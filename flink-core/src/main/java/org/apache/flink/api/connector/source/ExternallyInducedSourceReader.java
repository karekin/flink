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

package org.apache.flink.api.connector.source;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.PublicEvolving;

import java.util.Optional;

/**
 * 实现此接口的源读取器会在从检查点协调器收到触发消息时延迟检查点，直到其输入数据/事件表明应触发检查点。
 * <p>
 * {@link #shouldTriggerCheckpoint()} 被调用时，{@link ExternallyInducedSourceReader} 告诉 Flink 运行时需要进行检查点。
 * <p>
 * 通常情况下，这类源读取器会与 {@link SplitEnumerator} 一起工作，由其通知外部系统触发检查点。
 * 外部系统还需要将 Checkpoint ID 转发给源读取器，以便源读取器知道需要触发哪个检查点。
 * <p>
 * <b>重要：</b> 所有并行源任务必须大致在同一时间触发检查点。
 * 否则，由于长时间的检查点对齐阶段或大型对齐数据快照，会导致性能问题。
 *
 * @param <T>        源读取器产生的记录类型。
 * @param <SplitT>   源读取器处理的分片类型。
 */
@Experimental
@PublicEvolving
public interface ExternallyInducedSourceReader<T, SplitT extends SourceSplit>
        extends SourceReader<T, SplitT> {

    /**
     * 一个方法，用于告知 Flink 运行时是否需要在此源上触发检查点。
     * <p>
     * 当之前的 {@link #pollNext(ReaderOutput)} 返回 {@link org.apache.flink.core.io.InputStatus#NOTHING_AVAILABLE} 时，
     * 会调用此方法，以检查源是否需要进行检查点。
     * <p>
     * 如果返回 CheckpointId，则 Flink 运行时将在该源读取器上触发检查点。
     * 否则，Flink 运行时将继续处理记录。
     *
     * @return Flink 运行时应为其触发检查点的可选 Checkpoint ID。
     */
    Optional<Long> shouldTriggerCheckpoint();
}
