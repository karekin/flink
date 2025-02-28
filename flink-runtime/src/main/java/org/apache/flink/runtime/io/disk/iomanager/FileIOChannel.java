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

package org.apache.flink.runtime.io.disk.iomanager;

import org.apache.flink.util.StringUtils;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * FileIOChannel表示逻辑上属于同一资源的一组文件。例如，包含来自同一数据流的已排序数据块的文件集合，这些数据块稍后将被一起合并。
 */
public interface FileIOChannel {

    /**
     * 获取此I/O通道的通道ID。
     *
     * @return 通道ID。
     */
    ID getChannelID();

    /** 获取底层文件的大小（以字节为单位）。 */
    long getSize() throws IOException;

    /**
     * 检查通道是否已关闭。
     *
     * @return 如果通道已关闭，则返回true；否则返回false。
     */
    boolean isClosed();

    /**
     * 关闭通道。对于异步实现，此方法会等待所有待处理的请求完成。即使异常中断了关闭过程，底层的 FileChannel 也会关闭。
     *
     * @throws IOException 如果在等待待处理请求时发生错误。
     */
    void close() throws IOException;

    /**
     * 删除底层文件。
     *
     * @throws IllegalStateException 如果通道仍然打开。
     */
    void deleteChannel();

    FileChannel getNioFileChannel();

    /**
     * 关闭通道并删除底层文件。对于异步实现，此方法会等待所有待处理的请求完成。
     *
     * @throws IOException 如果在等待待处理请求时发生错误。
     */
    void closeAndDelete() throws IOException;

    // ----------------------------------------------------------------------------------------
    // ----------------------------------------------------------------------------------------

    /** 用于标识底层文件通道的ID。 */
    class ID {

        private static final int RANDOM_BYTES_LENGTH = 16;

        private final File path;

        private final int threadNum;

        private ID(File path, int threadNum) {
            this.path = path;
            this.threadNum = threadNum;
        }

        public ID(File basePath, int threadNum, Random random) {
            this.path = new File(basePath, randomString(random) + ".channel");
            this.threadNum = threadNum;
        }

        /** 返回底层临时文件的路径。 */
        public String getPath() {
            return path.getAbsolutePath();
        }

        /** 返回底层临时文件的路径作为File对象。 */
        public File getPathFile() {
            return path;
        }

        int getThreadNum() {
            return this.threadNum;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof ID) {
                ID other = (ID) obj;
                return this.path.equals(other.path) && this.threadNum == other.threadNum;
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            return path.hashCode();
        }

        @Override
        public String toString() {
            return path.getAbsolutePath();
        }

        private static String randomString(Random random) {
            byte[] bytes = new byte[RANDOM_BYTES_LENGTH];
            random.nextBytes(bytes);
            return StringUtils.byteToHexString(bytes);
        }
    }

    /** 用于枚举逻辑上属于一组的通道。 */
    final class Enumerator {

        private static AtomicInteger globalCounter = new AtomicInteger();

        private final File[] paths;

        private final String namePrefix;

        private int localCounter;

        public Enumerator(File[] basePaths, Random random) {
            this.paths = basePaths;
            this.namePrefix = ID.randomString(random);
            this.localCounter = 0;
        }

        public ID next() {
            // 本地计数器用于递增文件名，而全局计数器用于索引目录和关联的读写线程。这在所有溢出操作符之间进行循环操作，避免I/O堆积。
            int threadNum = globalCounter.getAndIncrement() % paths.length;
            String filename = String.format("%s.%06d.channel", namePrefix, (localCounter++));
            return new ID(new File(paths[threadNum], filename), threadNum);
        }
    }
}
