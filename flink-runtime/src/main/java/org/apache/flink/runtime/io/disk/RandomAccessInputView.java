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

package org.apache.flink.runtime.io.disk;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.SeekableDataInputView;
import org.apache.flink.runtime.memory.AbstractPagedInputView;
import org.apache.flink.util.MathUtils;

import java.io.EOFException;
import java.util.ArrayList;

/**
 * 随机访问输入视图，支持分段内存管理。
 */
public class RandomAccessInputView extends AbstractPagedInputView implements SeekableDataInputView {

    private final ArrayList<MemorySegment> segments;  // 内存段列表
    private int currentSegmentIndex;  // 当前所在的分段索引
    private final int segmentSizeBits;  // 分段大小的比特数
    private final int segmentSizeMask;  // 分段大小掩码
    private final int segmentSize; // 每个分段的大小
    private final int limitInLastSegment;  // 最后一个分段的大小限制

    /**
     * 构造函数，初始化随机访问输入视图。
     *
     * @param segments 内存段列表
     * @param segmentSize 每个分段的大小
     */
    public RandomAccessInputView(ArrayList<MemorySegment> segments, int segmentSize) {
        this(segments, segmentSize, segmentSize);
    }

    /**
     * 构造函数，初始化随机访问输入视图，支持指定最后一个分段的大小。
     *
     * @param segments 内存段列表
     * @param segmentSize 每个分段的大小
     * @param limitInLastSegment 最后一个分段的大小限制
     */
    public RandomAccessInputView(
            ArrayList<MemorySegment> segments, int segmentSize, int limitInLastSegment) {
        // 调用父类的构造函数，初始化当前分段、长度限制和当前偏移
        super(segments.get(0), segments.size() > 1 ? segmentSize : limitInLastSegment, 0);
        this.segments = segments;
        this.currentSegmentIndex = 0;
        this.segmentSize = segmentSize;
        // 计算分段大小的比特数
        this.segmentSizeBits = MathUtils.log2strict(segmentSize);
        // 计算分段大小掩码，用于位运算
        this.segmentSizeMask = segmentSize - 1;
        this.limitInLastSegment = limitInLastSegment;
    }

    /**
     * 设置读取位置。
     *
     * @param position 要设置的读取位置
     */
    @Override
    public void setReadPosition(long position) {
        // 计算目标分段和偏移量
        final int bufferNum = (int) (position >>> this.segmentSizeBits);  // 分段索引
        final int offset = (int) (position & this.segmentSizeMask);  // 当前分段中的偏移量

        this.currentSegmentIndex = bufferNum;
        // 调用父类的 seekInput 方法，切换到目标分段并设置偏移量
        seekInput(
                this.segments.get(bufferNum),
                offset,
                bufferNum < this.segments.size() - 1 ? this.segmentSize : this.limitInLastSegment);
    }

    /**
     * 获取当前的读取位置。
     *
     * @return 当前的读取位置
     */
    public long getReadPosition() {
        return (((long) currentSegmentIndex) << segmentSizeBits) + getCurrentPositionInSegment();
    }

    /**
     * 获取下一个分段。
     *
     * @param current 当前分段
     * @return 下一个分段，如果不存在则抛出 EOFException
     * @throws EOFException 如果没有更多分段
     */
    @Override
    protected MemorySegment nextSegment(MemorySegment current) throws EOFException {
        if (++this.currentSegmentIndex < this.segments.size()) {
            return this.segments.get(this.currentSegmentIndex);
        } else {
            throw new EOFException();
        }
    }

    /**
     * 获取当前分段的大小限制。
     *
     * @param segment 当前分段
     * @return 当前分段的大小限制
     */
    @Override
    protected int getLimitForSegment(MemorySegment segment) {
        return this.currentSegmentIndex == this.segments.size() - 1
                ? this.limitInLastSegment
                : this.segmentSize;
    }
}
