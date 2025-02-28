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
import org.apache.flink.runtime.io.disk.RandomAccessInputView;
import org.apache.flink.runtime.memory.AbstractPagedOutputView;
import org.apache.flink.runtime.operators.sort.IndexedSortable;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.generated.NormalizedKeyComputer;
import org.apache.flink.table.runtime.generated.RecordComparator;
import org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer;
import org.apache.flink.table.runtime.util.MemorySegmentPool;

import java.io.IOException;
import java.util.ArrayList;

/**
 * 抽象可排序类，提供基本的比较和交换功能。支持索引和规范化键的写入。
 */
public abstract class BinaryIndexedSortable implements IndexedSortable {

    public static final int OFFSET_LEN = 8; // 偏移量长度为8字节

    // 特征部分：将规范化键写入并比较
    private final NormalizedKeyComputer normalizedKeyComputer; // 规范化键计算器
    protected final BinaryRowDataSerializer serializer; // 序列化器

    // 如果规范化键无法完全确定记录顺序，则需要比较记录
    private final RecordComparator comparator; // 记录比较器

    // 数据存储部分
    protected final RandomAccessInputView recordBuffer; // 数据缓冲区
    private final RandomAccessInputView recordBufferForComparison; // 用于比较的辅助数据缓冲区

    // 索引存储部分
    protected MemorySegment currentSortIndexSegment; // 当前排序索引段
    protected final MemorySegmentPool memorySegmentPool; // 内存段池
    protected final ArrayList<MemorySegment> sortIndex; // 排序索引

    // 规范化键相关属性
    private final int numKeyBytes; // 规范化键的字节数
    protected final int indexEntrySize; // 索引条目大小
    protected final int indexEntriesPerSegment; // 每个内存段的索引条目数
    protected final int lastIndexEntryOffset; // 每个内存段的最后一个索引条目偏移量
    private final boolean normalizedKeyFullyDetermines; // 规范化键是否完全确定记录顺序
    private final boolean useNormKeyUninverted; // 是否使用未反转的规范化键

    // 序列化比较相关
    protected final BinaryRowDataSerializer serializer1; // 序列化器副本
    private final BinaryRowDataSerializer serializer2; // 序列化器副本
    protected final BinaryRowData row1; // 临时行数据
    private final BinaryRowData row2; // 临时行数据

    // 运行时变量
    protected int currentSortIndexOffset; // 当前排序索引偏移量
    protected int numRecords; // 数据记录数

    /**
     * 构造方法，初始化相关组件。
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     */
    public BinaryIndexedSortable(
            NormalizedKeyComputer normalizedKeyComputer,
            BinaryRowDataSerializer serializer,
            RecordComparator comparator,
            ArrayList<MemorySegment> recordBufferSegments,
            MemorySegmentPool memorySegmentPool) {
        if (normalizedKeyComputer == null || serializer == null) { // 参数非空检查
            throw new NullPointerException("NormalizedKeyComputer 或 serializer不能为 null.");
        }

        this.normalizedKeyComputer = normalizedKeyComputer; // 初始化规范化键计算器
        this.serializer = serializer; // 初始化序列化器
        this.comparator = comparator; // 初始化记录比较器
        this.memorySegmentPool = memorySegmentPool; // 初始化内存段池

        this.useNormKeyUninverted = !normalizedKeyComputer.invertKey(); // 是否使用未反转的规范化键

        this.numKeyBytes = normalizedKeyComputer.getNumKeyBytes(); // 获取规范化键的字节数

        int segmentSize = memorySegmentPool.pageSize(); // 获取内存段大小
        this.recordBuffer = new RandomAccessInputView(recordBufferSegments, segmentSize); // 初始化数据缓冲区
        this.recordBufferForComparison =
                new RandomAccessInputView(recordBufferSegments, segmentSize); // 初始化用于比较的辅助数据缓冲区

        this.normalizedKeyFullyDetermines = normalizedKeyComputer.isKeyFullyDetermines(); // 规范化键是否完全确定记录顺序

        // 计算索引条目大小和限制
        this.indexEntrySize = numKeyBytes + OFFSET_LEN;
        this.indexEntriesPerSegment = segmentSize / this.indexEntrySize;
        this.lastIndexEntryOffset = (this.indexEntriesPerSegment - 1) * this.indexEntrySize;

        // 初始化序列化器
        this.serializer1 = (BinaryRowDataSerializer) serializer.duplicate();
        this.serializer2 = (BinaryRowDataSerializer) serializer.duplicate();
        this.row1 = this.serializer1.createInstance(); // 创建临时行数据
        this.row2 = this.serializer2.createInstance(); // 创建临时行数据

        // 设置初始状态
        this.sortIndex = new ArrayList<>(16); // 初始化排序索引列表
        this.currentSortIndexSegment = nextMemorySegment(); // 获取下一个内存段作为当前排序索引段
        this.sortIndex.add(this.currentSortIndexSegment); // 添加到排序索引列表
        this.currentSortIndexOffset = 0; // 初始化当前排序索引偏移量
        this.numRecords = 0; // 初始化数据记录数
    }

    /**
     * 获取下一个内存段。
     */
    protected MemorySegment nextMemorySegment() {
        return this.memorySegmentPool.nextSegment(); // 从内存段池中获取下一个内存段
    }

    /**
     * 检查是否需要请求下一个索引内存。
     */
    protected boolean checkNextIndexOffset() {
        if (this.currentSortIndexOffset > this.lastIndexEntryOffset) { // 当前偏移量是否超出当前内存段
            MemorySegment returnSegment = nextMemorySegment(); // 获取下一个内存段
            if (returnSegment != null) { // 如果存在内存段
                this.currentSortIndexSegment = returnSegment; // 更新当前排序索引段
                this.sortIndex.add(this.currentSortIndexSegment); // 添加到排序索引列表
                this.currentSortIndexOffset = 0; // 重置当前排序索引偏移量
                return true; // 返回true，表示成功切换到下一个内存段
            } else {
                return false; // 返回false，表示没有更多内存段可用
            }
        }
        return true; // 当前内存段还有空间，返回true
    }

    /**
     * 将记录的偏移量和规范化键写入索引。
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     */
    protected void writeIndexAndNormalizedKey(RowData record, long currOffset) {
        // 添加指向数据的偏移量和规范化键
        this.currentSortIndexSegment.putLong(this.currentSortIndexOffset, currOffset); // 写入数据偏移量

        if (this.numKeyBytes != 0) { // 如果规范化键的字节数不为零
            normalizedKeyComputer.putKey( // 写入规范化键
                    record,
                    this.currentSortIndexSegment,
                    this.currentSortIndexOffset + OFFSET_LEN);
        }

        this.currentSortIndexOffset += this.indexEntrySize; // 更新当前排序索引偏移量
        this.numRecords++; // 数据记录数加一
    }

    /**
     * 比较两个索引项。
     */
    @Override
    public int compare(int i, int j) {
        // 将索引转换为内存段和偏移量
        final int segmentNumberI = i / this.indexEntriesPerSegment;
        final int segmentOffsetI = (i % this.indexEntriesPerSegment) * this.indexEntrySize;

        final int segmentNumberJ = j / this.indexEntriesPerSegment;
        final int segmentOffsetJ = (j % this.indexEntriesPerSegment) * this.indexEntrySize;

        return compare( // 调用比较方法
                segmentNumberI,
                segmentOffsetI,
                segmentNumberJ,
                segmentOffsetJ);
    }

    /**
     * 比较两个索引项。
     */
    @Override
    public int compare(int segmentNumberI, int segmentOffsetI, int segmentNumberJ, int segmentOffsetJ) {
        final MemorySegment segI = this.sortIndex.get(segmentNumberI); // 获取第一个索引项的内存段
        final MemorySegment segJ = this.sortIndex.get(segmentNumberJ); // 获取第二个索引项的内存段

        // 比较规范化键
        int val = normalizedKeyComputer.compareKey(
                segI,
                segmentOffsetI + OFFSET_LEN,
                segJ,
                segmentOffsetJ + OFFSET_LEN);

        if (val != 0 || this.normalizedKeyFullyDetermines) { // 如果规范化键不同，或者规范化键完全确定记录顺序
            return this.useNormKeyUninverted ? val : -val; // 返回比较结果
        }

        // 如果规范化键相同，需要比较记录本身
        final long pointerI = segI.getLong(segmentOffsetI); // 获取第一个记录的偏移量
        final long pointerJ = segJ.getLong(segmentOffsetJ); // 获取第二个记录的偏移量

        return compareRecords(pointerI, pointerJ); // 比较记录
    }

    private int compareRecords(long pointer1, long pointer2) {
        this.recordBuffer.setReadPosition(pointer1); // 设置第一个记录的读取位置
        this.recordBufferForComparison.setReadPosition(pointer2); // 设置第二个记录的读取位置

        try {
            // 比较两个记录
            return this.comparator.compare(
                    serializer1.mapFromPages(row1, recordBuffer),
                    serializer2.mapFromPages(row2, recordBufferForComparison));
        } catch (IOException ioex) {
            throw new RuntimeException("比较两个记录时发生错误.", ioex);
        }
    }

    /**
     * 交换两个索引项。
     */
    @Override
    public void swap(int i, int j) {
        // 将索引转换为内存段和偏移量
        final int segmentNumberI = i / this.indexEntriesPerSegment;
        final int segmentOffsetI = (i % this.indexEntriesPerSegment) * this.indexEntrySize;

        final int segmentNumberJ = j / this.indexEntriesPerSegment;
        final int segmentOffsetJ = (j % this.indexEntriesPerSegment) * this.indexEntrySize;

        swap( // 调用交换方法
                segmentNumberI,
                segmentOffsetI,
                segmentNumberJ,
                segmentOffsetJ);
    }

    /**
     * 交换两个索引项。
     */
    @Override
    public void swap(int segmentNumberI, int segmentOffsetI, int segmentNumberJ, int segmentOffsetJ) {
        final MemorySegment segI = this.sortIndex.get(segmentNumberI); // 获取第一个索引项的内存段
        final MemorySegment segJ = this.sortIndex.get(segmentNumberJ); // 获取第二个索引项的内存段

        // 交换偏移量
        long index = segI.getLong(segmentOffsetI); // 获取偏移量
        segI.putLong(segmentOffsetI, segJ.getLong(segmentOffsetJ)); // 将第二个偏移量写入第一个位置
        segJ.putLong(segmentOffsetJ, index); // 将第一个偏移量写入第二个位置

        // 交换规范化键
        normalizedKeyComputer.swapKey( // 交换规范化键
                segI,
                segmentOffsetI + OFFSET_LEN,
                segJ,
                segmentOffsetJ + OFFSET_LEN);
    }

    /**
     * 获取总记录数。
     */
    @Override
    public int size() {
        return this.numRecords;
    }

    /**
     * 获取索引条目大小。
     */
    @Override
    public int recordSize() {
        return this.indexEntrySize;
    }

    /**
     * 获取每个内存段的记录数。
     */
    @Override
    public int recordsPerSegment() {
        return this.indexEntriesPerSegment;
    }

    /**
     * 将所有记录写入输出视图。
     */
    public void writeToOutput(AbstractPagedOutputView output) throws IOException {
        final int numRecords = this.numRecords; // 获取总记录数
        int currentMemSeg = 0; // 当前内存段索引
        int currentRecord = 0; // 当前记录索引

        while (currentRecord < numRecords) { // 遍历所有记录
            final MemorySegment currentIndexSegment = this.sortIndex.get(currentMemSeg++); // 获取当前内存段

            // 遍历当前内存段中的所有记录
            for (int offset = 0;
                 currentRecord < numRecords && offset <= this.lastIndexEntryOffset;
                 currentRecord++, offset += this.indexEntrySize) {
                final long pointer = currentIndexSegment.getLong(offset); // 获取记录的偏移量
                this.recordBuffer.setReadPosition(pointer); // 设置数据缓冲区的读取位置
                this.serializer.copyFromPagesToView(this.recordBuffer, output); // 将记录数据写入输出视图
            }
        }
    }
}
