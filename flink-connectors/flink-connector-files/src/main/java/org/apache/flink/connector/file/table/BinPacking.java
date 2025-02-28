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

package org.apache.flink.connector.file.table;

import org.apache.flink.annotation.Internal;

import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;

/**
 * 一个装箱实现。
 * <p> 装箱问题是指将不同大小的物品装入固定容量的箱子，尽量减少箱子的数量。
 * 这个类通过遍历物品并将它们分配到合适的箱子中来实现装箱算法。
 */
@Internal
public class BinPacking {
    private BinPacking() {}

    /**
     * 将物品装入箱子中，每个箱子的目标重量是 targetWeight。
     * 物品按顺序被装入第一个可以容纳它们的箱子。
     *
     * @param items        待装箱的物品
     * @param weightFunc   获取物品重量的函数
     * @param targetWeight 每个箱子的目标重量
     * @return 装箱后的物品列表
     */
    public static <T> List<List<T>> pack(
            Iterable<T> items, Function<T, Long> weightFunc, long targetWeight) {
        List<List<T>> packed = new ArrayList<>();
        Deque<Bin<T>> bins = new LinkedList<>();

        for (T item : items) {
            // 获取物品的重量
            long weight = weightFunc.apply(item);
            // 查找可以容纳该物品的箱子
            Bin<T> bin = findBin(bins, weight);

            if (bin != null) {
                // 将物品添加到箱子中
                bin.add(item, weight);
            } else {
                // 创建新箱子并将物品添加进去
                bin = new Bin<>(targetWeight);
                bin.add(item, weight);
                bins.addLast(bin);

                // 避免算法复杂度过高
                if (bins.size() > 10) {
                    // 每当箱子数超过10个时，将最老的箱子打包到结果中
                    packed.add(bins.removeFirst().items());
                }
            }
        }

        // 将剩余的箱子打包到结果中
        bins.forEach(bin -> packed.add(bin.items));
        return packed;
    }

    /**
     * 查找可以容纳指定重量的箱子。
     *
     * @param bins   箱子列表
     * @param weight 需要容纳的重量
     * @return 可以容纳指定重量的箱子，如果不存在则返回 null
     */
    private static <T> Bin<T> findBin(Iterable<Bin<T>> bins, long weight) {
        for (Bin<T> bin : bins) {
            // 检查箱子是否可以容纳指定重量
            if (bin.canAdd(weight)) {
                return bin;
            }
        }
        return null;
    }

    /**
     * 代表一个箱子，用于存储物品。
     */
    private static class Bin<T> {
        /**
         * 箱子的目标重量。
         */
        private final long targetWeight;

        /**
         * 箱子中存储的物品列表。
         */
        private final List<T> items = new ArrayList<>();

        /**
         * 箱子的当前重量。
         */
        private long binWeight = 0L;

        /**
         * 创建一个箱子，并指定目标重量。
         *
         * @param targetWeight 箱子的目标重量
         */
        Bin(long targetWeight) {
            this.targetWeight = targetWeight;
        }

        /**
         * 获取箱子中存储的物品列表。
         *
         * @return 物品列表
         */
        List<T> items() {
            return items;
        }

        /**
         * 检查箱子是否可以容纳指定重量。
         *
         * @param weight 需要容纳的重量
         * @return 如果箱子可以容纳指定重量则返回 true，否则返回 false
         */
        boolean canAdd(long weight) {
            return binWeight + weight <= targetWeight;
        }

        /**
         * 向箱子中添加物品。
         *
         * @param item   待添加的物品
         * @param weight 物品的重量
         */
        void add(T item, long weight) {
            this.binWeight += weight;
            items.add(item);
        }
    }
}
