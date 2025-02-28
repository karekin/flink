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

package org.apache.flink.connector.file.src.impl;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.PendingSplitsCheckpoint;
import org.apache.flink.connector.file.src.assigners.FileSplitAssigner;
import org.apache.flink.connector.file.src.enumerate.FileEnumerator;
import org.apache.flink.core.fs.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * 一个连续监控型的分片枚举器。
 * <p>该枚举器主要用于文件分片的连续监控与分配，适用于分布式文件数据源的分片管理与任务分配。</p>
 */
@Internal
public class ContinuousFileSplitEnumerator
        implements SplitEnumerator<FileSourceSplit, PendingSplitsCheckpoint<FileSourceSplit>> {

    private static final Logger LOG = LoggerFactory.getLogger(ContinuousFileSplitEnumerator.class);

    private final SplitEnumeratorContext<FileSourceSplit> context; // 分片枚举器上下文，用于管理分片的分配和消费者的状态

    private final FileSplitAssigner splitAssigner; // 文件分片分配器，负责将分片分配给不同的消费者

    private final FileEnumerator enumerator; // 文件枚举器，用于发现和枚举文件分片

    private final HashSet<Path> pathsAlreadyProcessed; // 已经处理过的文件路径集合

    private final LinkedHashMap<Integer, String> readersAwaitingSplit; // 等待分片的消费者集合，键为消费者的子任务ID，值为消费者的主机名

    private final Path[] paths; // 需要监控的文件路径数组

    private final long discoveryInterval; // 发现分片的间隔时间（毫秒）

    // ------------------------------------------------------------------------

    public ContinuousFileSplitEnumerator(
            SplitEnumeratorContext<FileSourceSplit> context,
            FileEnumerator enumerator,
            FileSplitAssigner splitAssigner,
            Path[] paths,
            Collection<Path> alreadyDiscoveredPaths,
            long discoveryInterval) {

        checkArgument(discoveryInterval > 0L); // 确保发现间隔时间大于0
        this.context = checkNotNull(context); // 初始化分片枚举器上下文
        this.enumerator = checkNotNull(enumerator); // 初始化文件枚举器
        this.splitAssigner = checkNotNull(splitAssigner); // 初始化分片分配器
        this.paths = paths; // 保存需要监控的文件路径数组
        this.discoveryInterval = discoveryInterval; // 保存发现间隔时间
        this.pathsAlreadyProcessed = new HashSet<>(alreadyDiscoveredPaths); // 初始化已经处理过的文件路径集合
        this.readersAwaitingSplit = new LinkedHashMap<>(); // 初始化等待分片的消费者集合
    }

    @Override
    public void start() {
        // 调用异步方法枚举分片，并设置定时触发
        // 如果已经有分片被发现，则会触发 processDiscoveredSplits 方法
        context.callAsync(
                () -> enumerator.enumerateSplits(paths, 1), // 枚举分片的方法
                this::processDiscoveredSplits, // 处理发现的分片的回调方法
                discoveryInterval, // 初始延迟时间
                discoveryInterval); // 发现间隔时间
    }

    @Override
    public void close() throws IOException {
        // 无资源需要关闭
    }

    @Override
    public void addReader(int subtaskId) {
        // 该源是一个基于懒惰拉取机制的纯源，无需在注册时执行任何操作
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        // 将请求分片的消费者添加到等待列表中
        readersAwaitingSplit.put(subtaskId, requesterHostname);
        // 分配分片
        assignSplits();
    }

    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
        // 处理来自消费者的消息
        LOG.error("收到无法识别的消息: {}", sourceEvent);
    }

    @Override
    public void addSplitsBack(List<FileSourceSplit> splits, int subtaskId) {
        // 记录需要重新添加的分片
        LOG.debug("File Source Enumerator 重复添加分片: {}", splits);
        splitAssigner.addSplits(splits); // 将分片重新添加到待分配队列
    }

    @Override
    public PendingSplitsCheckpoint<FileSourceSplit> snapshotState(long checkpointId)
            throws Exception {
        // 保存当前剩余分片和已处理路径的检查点
        final PendingSplitsCheckpoint<FileSourceSplit> checkpoint =
                PendingSplitsCheckpoint.fromCollectionSnapshot(
                        splitAssigner.remainingSplits(), pathsAlreadyProcessed);

        LOG.debug("Source Checkpoint is {}", checkpoint);
        return checkpoint;
    }

    // ------------------------------------------------------------------------

    private void processDiscoveredSplits(Collection<FileSourceSplit> splits, Throwable error) {
        if (error != null) {
            // 如果分片发现过程中出现错误，记录日志
            LOG.error("枚举文件失败", error);
            return;
        }

        // 过滤掉已经被处理过的路径，并收集新的分片
        final Collection<FileSourceSplit> newSplits =
                splits.stream()
                        .filter((split) -> pathsAlreadyProcessed.add(split.path())) // 如果路径未被处理过，则添加到已处理路径集合
                        .collect(Collectors.toList());
        splitAssigner.addSplits(newSplits); // 将新的分片添加到待分配队列

        // 分配分片
        assignSplits();
    }

    private void assignSplits() {
        final Iterator<Map.Entry<Integer, String>> awaitingReader =
                readersAwaitingSplit.entrySet().iterator();

        while (awaitingReader.hasNext()) {
            final Map.Entry<Integer, String> nextAwaiting = awaitingReader.next();

            // 如果请求分片的消费者已经故障，则从等待列表中移除
            if (!context.registeredReaders().containsKey(nextAwaiting.getKey())) {
                awaitingReader.remove();
                continue;
            }

            final String hostname = nextAwaiting.getValue(); // 消费者的主机名
            final int awaitingSubtask = nextAwaiting.getKey(); // 消费者的子任务ID
            final Optional<FileSourceSplit> nextSplit = splitAssigner.getNext(hostname); // 从分配器中获取下一个分片

            if (nextSplit.isPresent()) {
                // 如果存在分片，则分配给对应的消费者，并从等待列表中移除
                context.assignSplit(nextSplit.get(), awaitingSubtask);
                awaitingReader.remove();
            } else {
                // 如果没有分片可分配，则停止分配
                break;
            }
        }
    }
}
