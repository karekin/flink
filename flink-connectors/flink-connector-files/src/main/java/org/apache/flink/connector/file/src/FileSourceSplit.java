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

package org.apache.flink.connector.file.src;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.connector.file.src.util.CheckpointedPosition;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.StringUtils;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * 一个 {@link SourceSplit}，表示一个文件或文件的一部分。
 * <p>
 * 分片具有一个偏移量和结束位置，定义了分片所表示的文件区域。对于表示整个文件的分片，偏移量为零，长度为文件大小。
 * <p>
 * 分片还可能具有一个“读取器位置”，这是之前读取此分片的读取器的检查点位置。当分片从枚举器分配给读取器时，此位置通常为 null，而当读取器在文件源分片中检查点其状态时，此位置则为非 null。
 * <p>
 * 该类是 {@link Serializable}，以便于使用。对于 Flink 的内部序列化（无论是用于 RPC 还是检查点），使用 {@link FileSourceSplitSerializer}。
 */
@PublicEvolving
public class FileSourceSplit implements SourceSplit, Serializable {

    private static final long serialVersionUID = 1L; // 序列化版本号

    private static final String[] NO_HOSTS = StringUtils.EMPTY_STRING_ARRAY; // 默认无主机信息

    /** 分片的唯一 ID，在此数据源范围内唯一。 */
    private final String id;

    /** 该分片引用的文件路径。 */
    private final Path filePath;

    /** 文件中要处理的第一个字节的位置（包含）。 */
    private final long offset;

    /** 要处理的字节数（从偏移量开始）。 */
    private final long length;

    /** 文件的修改时间，来自 {@link FileStatus#getModificationTime()}。 */
    private final long fileModificationTime;

    /** 文件大小（字节），来自 {@link FileStatus#getLen()}。 */
    private final long fileSize;

    /**
     * 存储此文件范围的主机名。如果没有主机信息，则为空。
     */
    private final String[] hostnames;

    /** 读取器在分片中的精确位置，用于从中断点恢复。 */
    @Nullable private final CheckpointedPosition readerPosition;

    /**
     * 分片经常被序列化到检查点中。缓存字节表示形式可以使重复序列化变得高效。
     * 该字段由 {@link FileSourceSplitSerializer} 使用。
     */
    @Nullable transient byte[] serializedFormCache;

    // --------------------------------------------------------------------------------------------

    /**
     * 构造一个带有主机信息的分片。
     *
     * @param id 此源分片的唯一 ID。
     * @param filePath 文件路径。
     * @param offset 分片在文件中的起始位置（包含）。
     * @param length 分片中的字节数（从偏移量开始）。
     * @param fileModificationTime 文件的修改时间。
     * @param fileSize 文件的总大小。
     */
    public FileSourceSplit(
            String id,
            Path filePath,
            long offset,
            long length,
            long fileModificationTime,
            long fileSize) {
        this(id, filePath, offset, length, fileModificationTime, fileSize, NO_HOSTS);
    }

    /**
     * 构造一个带有主机信息的分片。
     *
     * @param id 此源分片的唯一 ID。
     * @param filePath 文件路径。
     * @param offset 分片在文件中的起始位置（包含）。
     * @param length 分片中的字节数（从偏移量开始）。
     * @param fileModificationTime 文件的修改时间。
     * @param fileSize 文件的总大小。
     * @param hostnames 存储此分片文件范围的主机名。
     */
    public FileSourceSplit(
            String id,
            Path filePath,
            long offset,
            long length,
            long fileModificationTime,
            long fileSize,
            String... hostnames) {
        this(id, filePath, offset, length, fileModificationTime, fileSize, hostnames, null, null);
    }

    /**
     * 构造一个带有主机信息的分片。
     *
     * @param id 此源分片的唯一 ID。
     * @param filePath 文件路径。
     * @param offset 分片在文件中的起始位置（包含）。
     * @param length 分片中的字节数（从偏移量开始）。
     * @param fileModificationTime 文件的修改时间。
     * @param fileSize 文件的总大小。
     * @param hostnames 存储此分片文件范围的主机名。
     * @param readerPosition 读取器的位置（可选）。
     */
    public FileSourceSplit(
            String id,
            Path filePath,
            long offset,
            long length,
            long fileModificationTime,
            long fileSize,
            String[] hostnames,
            @Nullable CheckpointedPosition readerPosition) {
        this(
                id,
                filePath,
                offset,
                length,
                fileModificationTime,
                fileSize,
                hostnames,
                readerPosition,
                null);
    }

    /**
     * @deprecated 请使用 {@link #FileSourceSplit(String, Path, long, long, long, long)}。
     */
    @Deprecated
    public FileSourceSplit(String id, Path filePath, long offset, long length) {
        this(id, filePath, offset, length, 0, 0, NO_HOSTS);
    }

    /**
     * @deprecated 请使用 {@link #FileSourceSplit(String, Path, long, long, long, long, String...)}。
     */
    @Deprecated
    public FileSourceSplit(
            String id, Path filePath, long offset, long length, String... hostnames) {
        this(id, filePath, offset, length, 0, 0, hostnames, null, null);
    }

    /**
     * @deprecated 请使用 {@link #FileSourceSplit(String, Path, long, long, long, long, String[], CheckpointedPosition)}。
     */
    @Deprecated
    public FileSourceSplit(
            String id,
            Path filePath,
            long offset,
            long length,
            String[] hostnames,
            @Nullable CheckpointedPosition readerPosition) {
        this(id, filePath, offset, length, 0, 0, hostnames, readerPosition, null);
    }

    /**
     * 包级私有构造函数，用于序列化器直接缓存序列化形式。
     */
    FileSourceSplit(
            String id,
            Path filePath,
            long offset,
            long length,
            long fileModificationTime,
            long fileSize,
            String[] hostnames,
            @Nullable CheckpointedPosition readerPosition,
            @Nullable byte[] serializedForm) {
        this.fileModificationTime = fileModificationTime;
        this.fileSize = fileSize;

        checkArgument(offset >= 0, "offset must be >= 0"); // 检查偏移量是否合法
        checkArgument(length >= 0, "length must be >= 0"); // 检查长度是否合法
        checkNoNullHosts(hostnames); // 检查主机名数组是否合法

        this.id = checkNotNull(id); // 确保 ID 不为 null
        this.filePath = checkNotNull(filePath); // 确保文件路径不为 null
        this.offset = offset;
        this.length = length;
        this.hostnames = hostnames;
        this.readerPosition = readerPosition;
        this.serializedFormCache = serializedForm; // 缓存序列化形式
    }

    // ------------------------------------------------------------------------
    // 分片属性
    // ------------------------------------------------------------------------

    @Override
    public String splitId() {
        return id; // 返回分片的唯一 ID
    }

    /** 获取文件路径。 */
    public Path path() {
        return filePath;
    }

    /**
     * 返回此源分片引用的文件区域的起始位置。该位置是包含的，表示分片的第一个字节。
     */
    public long offset() {
        return offset;
    }

    /** 返回此源分片描述的文件区域中的字节数。 */
    public long length() {
        return length;
    }

    /** 返回文件的修改时间，来自 {@link FileStatus#getModificationTime()}。 */
    public long fileModificationTime() {
        return fileModificationTime;
    }

    /** 返回文件的总大小（字节），来自 {@link FileStatus#getLen()}。 */
    public long fileSize() {
        return fileSize;
    }

    /**
     * 获取存储此分片文件范围的主机名。如果没有主机信息，则返回空数组。
     * <p>
     * 主机信息通常仅在特定文件系统（如 HDFS）中可用。
     */
    public String[] hostnames() {
        return hostnames;
    }

    /**
     * 获取读取器的（检查点）位置（如果有）。此值通常在分片从枚举器分配给读取器时不存在，而在从检查点恢复分片时存在。
     */
    public Optional<CheckpointedPosition> getReaderPosition() {
        return Optional.ofNullable(readerPosition);
    }

    /**
     * 创建此分片的一个副本，其中检查点位置被给定的新位置替换。
     * <p>
     * <b>重要：</b> 添加额外信息到分片的子类必须重写此方法以返回该子类类型。
     * 这是文件源实现中的强制性要求。我们没有通过泛型强制执行此契约，因为这会导致泛型的使用变得非常复杂。
     */
    public FileSourceSplit updateWithCheckpointedPosition(@Nullable CheckpointedPosition position) {
        return new FileSourceSplit(
                id, filePath, offset, length, fileModificationTime, fileSize, hostnames, position);
    }

    // ------------------------------------------------------------------------
    // 工具方法
    // ------------------------------------------------------------------------

    @Override
    public String toString() {
        final String hosts =
                hostnames.length == 0 ? "(no host info)" : " hosts=" + Arrays.toString(hostnames);
        return String.format(
                "FileSourceSplit: %s [%d, %d) %s ID=%s position=%s",
                filePath, offset, offset + length, hosts, id, readerPosition);
    }

    private static void checkNoNullHosts(String[] hosts) {
        checkNotNull(hosts, "hostnames array must not be null"); // 检查主机名数组不为 null
        for (String host : hosts) {
            checkArgument(host != null, "the hostnames must not contain null entries"); // 检查主机名数组中没有 null 元素
        }
    }
}
