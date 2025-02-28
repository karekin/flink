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

package org.apache.flink.connector.file.src.assigners;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.file.src.FileSourceSplit;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Collection;
import java.util.Optional;

/**
 * {@code FileSplitAssigner} 负责决定哪些分片应由哪个节点处理。它决定了分片的处理顺序和本地性。
 * <p>
 * 该接口的主要作用是管理文件分片的分配逻辑，确保分片能够被正确地分配给各个处理节点，并且处理顺序和本地性符合预期。
 */
@PublicEvolving
public interface FileSplitAssigner {

    /**
     * 获取下一个分片。
     * <p>
     * 当此方法返回一个空的 {@code Optional} 时，表示分片集已经分配完毕，源将在读者完成当前分片后结束。
     *
     * @param hostname 节点的主机名，用于确定分片的本地性（可选参数）
     * @return 下一个分片（如果有的话）
     */
    Optional<FileSourceSplit> getNext(@Nullable String hostname);

    /**
     * 向分配器中添加一组分片。这通常发生在分片处理失败需要重新添加时，或者发现了新的分片。
     * <p>
     * 这个方法允许动态地将新的分片添加到分配器中，以便它们可以被后续的分配操作处理。
     *
     * @param splits 要添加的分片集合
     */
    void addSplits(Collection<FileSourceSplit> splits);

    /**
     * 获取分配器中剩余的分片。
     * <p>
     * 该方法返回分配器中尚未分配的所有分片，可以通过此集合了解当前还有哪些分片需要处理。
     *
     * @return 分配器中剩余的分片集合
     */
    Collection<FileSourceSplit> remainingSplits();

    // ------------------------------------------------------------------------

    /**
     * 创建 {@code FileSplitAssigner} 的工厂接口，允许 {@code FileSplitAssigner} 在运行时动态初始化，并且不需要被序列化。
     * <p>
     * 该接口定义了一个创建分配器实例的方法，可以根据初始分片集创建分配器。
     */
    @FunctionalInterface
    interface Provider extends Serializable {

        /**
         * 创建一个新的 {@code FileSplitAssigner}，初始时带有给定的分片集。
         *
         * @param initialSplits 初始分片集
         * @return 新创建的 {@code FileSplitAssigner} 实例
         */
        FileSplitAssigner create(Collection<FileSourceSplit> initialSplits);
    }
}
