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

import org.apache.flink.runtime.operators.sort.MergeIterator;
import org.apache.flink.runtime.operators.sort.PartialOrderPriorityQueue;
import org.apache.flink.table.runtime.generated.RecordComparator;
import org.apache.flink.util.MutableObjectIterator;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * `MergeIterator` 的二进制版本。使用 `RecordComparator` 比较记录。
 */
public class BinaryMergeIterator<Entry> implements MutableObjectIterator<Entry> {

    // 使用头部元素构建堆
    private final PartialOrderPriorityQueue<HeadStream<Entry>> heap; // 优先队列
    private HeadStream<Entry> currHead; // 当前头部

    /**
     * 构造函数，初始化合并迭代器。
     */
    public BinaryMergeIterator(
            List<MutableObjectIterator<Entry>> iterators, // 输入迭代器列表
            List<Entry> reusableEntries, // 可复用的条目列表
            Comparator<Entry> comparator // 比较器
    ) throws IOException {
        checkArgument(iterators.size() == reusableEntries.size()); // 检查输入迭代器和可复用条目数量是否一致

        // 初始化优先队列
        this.heap = new PartialOrderPriorityQueue<>( // 创建优先队列
                (o1, o2) -> comparator.compare(o1.getHead(), o2.getHead()), // 比较队列中的元素
                iterators.size() // 队列的初始容量
        );

        // 为每个输入迭代器创建 HeadStream 并添加到优先队列
        for (int i = 0; i < iterators.size(); i++) {
            this.heap.add(new HeadStream<>(iterators.get(i), reusableEntries.get(i)));
        }
    }

    @Override
    public Entry next(Entry reuse) throws IOException {
        // 忽略reuse参数，因为每个HeadStream有自己的可复用BinaryRowData
        return next();
    }

    @Override
    public Entry next() throws IOException {
        if (currHead != null) { // 如果当前头部不为空
            if (!currHead.nextHead()) { // 尝试获取下一个头部
                this.heap.poll(); // 移除当前头部
            } else {
                this.heap.adjustTop(); // 调整优先队列的顶部元素
            }
        }

        if (this.heap.size() > 0) { // 如果优先队列不为空
            currHead = this.heap.peek(); // 获取当前最小的HeadStream
            return currHead.getHead(); // 返回当前头部
        } else {
            return null; // 返回null表示没有更多数据
        }
    }

    private static final class HeadStream<Entry> { // 内部类
        private final MutableObjectIterator<Entry> iterator; // 输入迭代器
        private Entry head; // 当前头部

        private HeadStream(MutableObjectIterator<Entry> iterator, Entry head) throws IOException {
            this.iterator = iterator; // 初始化输入迭代器
            this.head = head; // 初始化当前头部
            if (!nextHead()) { // 初始化头部
                throw new IllegalStateException("无法初始化HeadStream");
            }
        }

        private Entry getHead() { // 获取当前头部
            return this.head;
        }

        private boolean nextHead() throws IOException { // 获取下一个头部
            return (this.head = this.iterator.next(head)) != null; // 返回是否成功获取到下一个元素
        }
    }
}
