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
import org.apache.flink.core.memory.MemorySegmentSource;
import org.apache.flink.runtime.memory.AbstractPagedOutputView;
import org.apache.flink.util.MathUtils;

import java.io.EOFException;
import java.io.IOException;
import java.util.List;

/**
 * 该列表始终包含所有已完全填满的内存段，以及当前正在填充的内存段。
 */
public class SimpleCollectingOutputView extends AbstractPagedOutputView {

    // 存放所有已装满的内存段和当前正在写入的内存段
    private final List<MemorySegment> fullSegments;

    // 内存段来源，用于获取新的内存段
    private final MemorySegmentSource memorySource;

    // segmentSize 对应的 log2 值，用于计算偏移等操作（移位运算）
    private final int segmentSizeBits;

    // 当前已经使用的内存段编号，用于计算 offset
    private int segmentNum;

    /**
     * 构造方法
     *
     * @param fullSegmentTarget 用于存放所有内存段的列表
     * @param memSource 从此对象获取新的内存段
     * @param segmentSize 单个内存段的大小
     */
    public SimpleCollectingOutputView(
            List<MemorySegment> fullSegmentTarget, MemorySegmentSource memSource, int segmentSize) {
        // 调用父类构造方法，获取第一个内存段并设定每段大小、初始位置为0
        super(memSource.nextSegment(), segmentSize, 0);
        // 将 segmentSize 转换为其 log2 值，用于后续移位运算
        this.segmentSizeBits = MathUtils.log2strict(segmentSize);
        // 初始化存放内存段的列表
        this.fullSegments = fullSegmentTarget;
        // 保存获取内存段的来源
        this.memorySource = memSource;
        // 将当前获取到的内存段添加到 fullSegments 列表中
        this.fullSegments.add(getCurrentSegment());
    }

    /**
     * 重置方法
     * 在重置前，要求 fullSegments 必须为空，否则抛出异常
     * 重置后，会重新获取新的段并将 segmentNum 重置为 0
     */
    public void reset() {
        if (this.fullSegments.size() != 0) {
            throw new IllegalStateException("The target list still contains memory segments.");
        }

        // 清理父类状态
        clear();
        try {
            // 重新获取第一个内存段
            advance();
        } catch (IOException ioex) {
            throw new RuntimeException("Error getting first segment for record collector.", ioex);
        }
        // 将段编号重置为0
        this.segmentNum = 0;
    }

    /**
     * 当当前段写满，需要获取下一个内存段时被调用
     *
     * @param current 当前的内存段
     * @param positionInCurrent 当前段的写位置
     * @return 获取到的新的内存段
     * @throws EOFException 当没有可用的内存段时抛出
     */
    @Override
    protected MemorySegment nextSegment(MemorySegment current, int positionInCurrent)
            throws EOFException {
        // 从 memorySource 获取下一个内存段
        final MemorySegment next = this.memorySource.nextSegment();
        if (next != null) {
            // 将获取到的内存段添加到列表，并段编号自增
            this.fullSegments.add(next);
            this.segmentNum++;
            return next;
        } else {
            // 若无法再获取内存段，则抛出 EOFException
            throw new EOFException("Can't collect further: memorySource depleted");
        }
    }

    /**
     * 获取当前写偏移量：由 segmentNum 和在当前段的写位置组合而成
     * segmentNum 左移 segmentSizeBits 位再加上当前在 segment 中的位置
     *
     * @return 当前的全局偏移量
     */
    public long getCurrentOffset() {
        return (((long) this.segmentNum) << this.segmentSizeBits) + getCurrentPositionInSegment();
    }
}

