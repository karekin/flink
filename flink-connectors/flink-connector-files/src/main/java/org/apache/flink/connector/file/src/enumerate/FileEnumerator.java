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

package org.apache.flink.connector.file.src.enumerate;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.core.fs.Path;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;

/**
 * {@code FileEnumerator} 的任务是发现所有要读取的文件，并将它们分割成一组 {@link FileSourceSplit}。
 * <p>
 * 这涵盖了可能的路径遍历、文件过滤（按名称或其他模式）以及决定是否将文件分割成多个分片，以及如何分割它们。
 * </p>
 */
@PublicEvolving
public interface FileEnumerator {

    /**
     * 为给定路径下的相关文件生成所有文件分片。{@code minDesiredSplits} 是一个可选的提示，表示为了充分利用并行性所需生成的分片数量。
     * <p>
     * 该方法用于生成文件分片，它会根据给定的路径和所需的最小分片数生成一组文件分片，这些分片可用于后续的数据读取和处理。
     * </p>
     *
     * @param paths 文件路径数组
     * @param minDesiredSplits 所需的最小分片数
     * @return 生成的文件分片集合
     * @throws IOException 如果在生成分片时发生 I/O 错误
     */
    Collection<FileSourceSplit> enumerateSplits(Path[] paths, int minDesiredSplits)
            throws IOException;

    // ------------------------------------------------------------------------

    /**
     * 用于创建 {@code FileEnumerator} 的工厂接口，允许 {@code FileEnumerator} 在运行时被动态初始化，并且不需要被序列化。
     * <p>
     * 该接口允许实现类提供一种方式来创建 {@code FileEnumerator} 的实例，并且可以通过实现该接口来实现非序列化类的初始化。
     * </p>
     */
    @FunctionalInterface
    interface Provider extends Serializable {

        FileEnumerator create();
    }
}
