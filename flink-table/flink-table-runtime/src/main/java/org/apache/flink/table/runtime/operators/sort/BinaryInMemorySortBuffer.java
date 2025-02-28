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

package org.apache.flink.table.runtime.operators.sort;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.SimpleCollectingOutputView;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.generated.NormalizedKeyComputer;
import org.apache.flink.table.runtime.generated.RecordComparator;
import org.apache.flink.table.runtime.typeutils.AbstractRowDataSerializer;
import org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer;
import org.apache.flink.table.runtime.util.MemorySegmentPool;
import org.apache.flink.util.MutableObjectIterator;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * 内存中的二进制行排序缓冲区。
 */
public final class BinaryInMemorySortBuffer extends BinaryIndexedSortable {

    private static final int MIN_REQUIRED_BUFFERS = 3;

    // 输入记录的序列化器
    private final AbstractRowDataSerializer<RowData> inputSerializer;

    // 用于存储记录缓冲区段的列表
    private final ArrayList<MemorySegment> recordBufferSegments;

    // 用于收集记录到缓冲区段中的输出视图
    private final SimpleCollectingOutputView recordCollector;

    // 总共可用的缓冲区数
    private final int totalNumBuffers;

    // 当前数据缓冲区的偏移量
    private long currentDataBufferOffset;

    // 排序索引占用的字节数
    private long sortIndexBytes;

    /**
     * 创建以插入方式工作的内存排序器。
     *
     * @param normalizedKeyComputer 规范化键计算器
     * @param inputSerializer 输入记录的序列化器
     * @param serializer 行数据序列化器
     * @param comparator 记录比较器
     * @param memoryPool 内存池
     */
    public static BinaryInMemorySortBuffer createBuffer(
            NormalizedKeyComputer normalizedKeyComputer,
            AbstractRowDataSerializer<RowData> inputSerializer,
            BinaryRowDataSerializer serializer,
            RecordComparator comparator,
            MemorySegmentPool memoryPool) {
        checkArgument(memoryPool.freePages() >= MIN_REQUIRED_BUFFERS);
        int totalNumBuffers = memoryPool.freePages();
        ArrayList<MemorySegment> recordBufferSegments = new ArrayList<>(16);
        return new BinaryInMemorySortBuffer(
                normalizedKeyComputer,
                inputSerializer,
                serializer,
                comparator,
                recordBufferSegments,
                new SimpleCollectingOutputView(
                        recordBufferSegments, memoryPool, memoryPool.pageSize()),
                memoryPool,
                totalNumBuffers);
    }

    private BinaryInMemorySortBuffer(
            NormalizedKeyComputer normalizedKeyComputer,
            AbstractRowDataSerializer<RowData> inputSerializer,
            BinaryRowDataSerializer serializer,
            RecordComparator comparator,
            ArrayList<MemorySegment> recordBufferSegments,
            SimpleCollectingOutputView recordCollector,
            MemorySegmentPool pool,
            int totalNumBuffers) {
        super(normalizedKeyComputer, serializer, comparator, recordBufferSegments, pool);
        this.inputSerializer = inputSerializer;
        this.recordBufferSegments = recordBufferSegments;
        this.recordCollector = recordCollector;
        this.totalNumBuffers = totalNumBuffers;
    }

    // -----------------------------------------------------------------------------
    // 内存段
    // -----------------------------------------------------------------------------

    /**
     * 将排序缓冲区重置为空状态。所有包含的数据将被丢弃。
     */
    public void reset() {

        // 重置所有偏移量
        this.numRecords = 0;
        this.currentSortIndexOffset = 0;
        this.currentDataBufferOffset = 0;
        this.sortIndexBytes = 0;

        // 返回所有内存段到内存池
        returnToSegmentPool();

        // 获取第一个缓冲区段
        this.currentSortIndexSegment = nextMemorySegment();
        this.sortIndex.add(this.currentSortIndexSegment);
        this.recordCollector.reset();
    }

    public void returnToSegmentPool() {
        // 将所有内存段返回到内存池
        this.memorySegmentPool.returnAll(this.sortIndex);
        this.memorySegmentPool.returnAll(this.recordBufferSegments);
        this.sortIndex.clear();
        this.recordBufferSegments.clear();
    }

    /**
     * 检查缓冲区是否为空。
     *
     * @return 如果没有记录，则返回 true；否则返回 false。
     */
    public boolean isEmpty() {
        return this.numRecords == 0;
    }

    public void dispose() {
        returnToSegmentPool();
    }

    public long getCapacity() {
        return ((long) this.totalNumBuffers) * memorySegmentPool.pageSize();
    }

    public long getOccupancy() {
        return this.currentDataBufferOffset + this.sortIndexBytes;
    }

    /**
     * 将给定记录写入排序缓冲区。写入的记录将作为最后一个逻辑位置。
     *
     * @param record 要写入的记录
     * @return 如果记录成功写入，则返回 true；如果排序缓冲区已满，则返回 false。
     * @throws IOException 如果在将记录序列化到缓冲区时发生错误。
     */
    public boolean write(RowData record) throws IOException {
        // 检查是否需要新的内存段来存储排序索引
        if (!checkNextIndexOffset()) {
            return false;
        }

        // 将记录序列化到数据缓冲区段中
        int skip;
        try {
            skip = this.inputSerializer.serializeToPages(record, this.recordCollector);
        } catch (EOFException e) {
            return false;
        }

        final long newOffset = this.recordCollector.getCurrentOffset();
        long currOffset = currentDataBufferOffset + skip;

        writeIndexAndNormalizedKey(record, currOffset);

        this.currentDataBufferOffset = newOffset;

        return true;
    }

    private BinaryRowData getRecordFromBuffer(BinaryRowData reuse, long pointer) throws IOException {
        this.recordBuffer.setReadPosition(pointer);
        return this.serializer.mapFromPages(reuse, this.recordBuffer);
    }

    // -----------------------------------------------------------------------------

    /**
     * 获取一个在缓冲区记录的逻辑顺序上的迭代器。
     *
     * @return 返回记录的逻辑顺序的迭代器。
     */
    public MutableObjectIterator<BinaryRowData> getIterator() {
        return new MutableObjectIterator<BinaryRowData>() {
            private final int size = size();
            private int current = 0;

            private int currentSegment = 0;
            private int currentOffset = 0;

            private MemorySegment currentIndexSegment = sortIndex.get(0);

            @Override
            public BinaryRowData next(BinaryRowData target) {
                if (this.current < this.size) {
                    this.current++;
                    if (this.currentOffset > lastIndexEntryOffset) {
                        this.currentOffset = 0;
                        this.currentIndexSegment = sortIndex.get(++this.currentSegment);
                    }

                    long pointer = this.currentIndexSegment.getLong(this.currentOffset);
                    this.currentOffset += indexEntrySize;

                    try {
                        return getRecordFromBuffer(target, pointer);
                    } catch (IOException ioe) {
                        throw new RuntimeException(ioe);
                    }
                } else {
                    return null;
                }
            }

            @Override
            public BinaryRowData next() {
                throw new RuntimeException("Not supported!");
            }
        };
    }
}
