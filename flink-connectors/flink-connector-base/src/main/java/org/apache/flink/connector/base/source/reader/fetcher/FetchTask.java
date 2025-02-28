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

package org.apache.flink.connector.base.source.reader.fetcher;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;

import java.io.IOException;
import java.util.Collection;
import java.util.function.Consumer;

/** The default fetch task that fetches the records into the element queue. */
@Internal
class FetchTask<E, SplitT extends SourceSplit> implements SplitFetcherTask {
    private final SplitReader<E, SplitT> splitReader;
    private final FutureCompletingBlockingQueue<RecordsWithSplitIds<E>> elementsQueue;
    private final Consumer<Collection<String>> splitFinishedCallback;
    private final int fetcherIndex;
    private volatile RecordsWithSplitIds<E> lastRecords;
    private volatile boolean wakeup;

    /**
     * 创建一个FetchTask实例。
     *
     * @param splitReader       用于从split中读取记录的读取器。
     * @param elementsQueue     存储记录的队列。
     * @param splitFinishedCallback   当split完成时调用的回调。
     * @param fetcherIndex      fetcher的索引。
     */
    FetchTask(
            SplitReader<E, SplitT> splitReader,
            FutureCompletingBlockingQueue<RecordsWithSplitIds<E>> elementsQueue,
            Consumer<Collection<String>> splitFinishedCallback,
            int fetcherIndex) {
        this.splitReader = splitReader;
        this.elementsQueue = elementsQueue;
        this.splitFinishedCallback = splitFinishedCallback;
        this.lastRecords = null;
        this.fetcherIndex = fetcherIndex;
        this.wakeup = false;
    }

    /**
     * 运行该任务，从splitReader中获取记录并将其放入元素队列。
     *
     * @return 指示该任务是否完成。
     * @throws IOException 如果在获取或处理记录时发生I/O错误。
     */
    @Override
    public boolean run() throws IOException {
        try {
            if (!isWakenUp() && lastRecords == null) {
                // 如果没有被唤醒并且没有lastRecords，从splitReader中获取记录。
                lastRecords = splitReader.fetch();
            }

            if (!isWakenUp()) {
                // 如果没有被唤醒，将lastRecords放入队列。
                // 这确保了处理已获取的记录的原子性。
                if (elementsQueue.put(fetcherIndex, lastRecords)) {
                    if (!lastRecords.finishedSplits().isEmpty()) {
                        // 如果有已完成的split，调用回调函数。
                        splitFinishedCallback.accept(lastRecords.finishedSplits());
                    }
                    lastRecords = null;
                }
            }
        } catch (InterruptedException e) {
            // 线程中断时抛出异常。
            throw new IOException("Source fetch execution was interrupted", e);
        } finally {
            // 清理可能的唤醒效果。
            if (isWakenUp()) {
                wakeup = false;
            }
        }
        // 返回值不重要。
        return true;
    }

    /**
     * 唤醒该任务，使其继续运行。
     */
    @Override
    public void wakeUp() {
        // 设置唤醒标志。
        wakeup = true;
        if (lastRecords == null) {
            // 分为两种情况：
            // 1. splitReader正在读取或即将读取记录。
            // 2. 记录已被入队并设置为null。
            // 在第一种情况下，唤醒split reader。在第二种情况下，下一次运行可能会被跳过。
            // 在任何情况下，队列中不会入队正在进行中的记录。
            splitReader.wakeUp();
        } else {
            // 如果任务正在阻塞入队，中断线程。
            elementsQueue.wakeUpPuttingThread(fetcherIndex);
        }
    }

    /**
     * 检查该任务是否被唤醒。
     * @return true表示被唤醒，false表示未被唤醒。
     */
    private boolean isWakenUp() {
        return wakeup;
    }

    /**
     * 返回该任务的字符串表示。
     * @return 一个字符串"FetchTask"。
     */
    @Override
    public String toString() {
        return "FetchTask";
    }
}
