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

import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel;
import org.apache.flink.util.CollectionUtil;

import java.io.Closeable;
import java.io.File;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * 溢出通道管理器，用于管理溢出文件通道的生命周期。
 */
public class SpillChannelManager implements Closeable {

    private final HashSet<FileIOChannel.ID> channels; // 存储所有通道ID
    private final HashSet<FileIOChannel> openChannels; // 存储已打开的通道

    private volatile boolean closed; // 标记是否已关闭

    public SpillChannelManager() {
        // 初始化通道和已打开通道的集合
        this.channels = CollectionUtil.newHashSetWithExpectedSize(64);
        this.openChannels = CollectionUtil.newHashSetWithExpectedSize(64);
    }

    /**
     * 添加一个新的文件通道。
     * @param id 通道ID
     */
    public synchronized void addChannel(FileIOChannel.ID id) {
        checkArgument(!closed); // 检查是否已关闭
        channels.add(id); // 将通道ID添加到未打开的通道集合中
    }

    /**
     * 打开文件通道。
     * @param toOpen 要打开的通道列表
     */
    public synchronized void addOpenChannels(List<FileIOChannel> toOpen) {
        checkArgument(!closed); // 检查是否已关闭
        for (FileIOChannel channel : toOpen) {
            openChannels.add(channel); // 将通道添加到已打开的通道集合中
            channels.remove(channel.getChannelID()); // 从未打开的通道集合中移除该通道ID
        }
    }

    /**
     * 从通道管理器中移除通道。
     * @param id 通道ID
     */
    public synchronized void removeChannel(FileIOChannel.ID id) {
        checkArgument(!closed); // 检查是否已关闭
        channels.remove(id); // 从未打开的通道集合中移除该通道ID
    }

    @Override
    public synchronized void close() {
        if (this.closed) {
            return; // 如果已关闭，直接返回
        }

        this.closed = true; // 标记为已关闭

        // 关闭所有已打开的通道
        for (Iterator<FileIOChannel> channels = this.openChannels.iterator(); channels.hasNext();) {
            final FileIOChannel channel = channels.next();
            channels.remove(); // 从已打开的通道集合中移除
            try {
                channel.closeAndDelete(); // 关闭并删除通道
            } catch (Throwable ignored) {
            }
        }

        // 删除所有未打开的通道
        for (Iterator<FileIOChannel.ID> channels = this.channels.iterator(); channels.hasNext();) {
            final FileIOChannel.ID channel = channels.next();
            channels.remove(); // 从未打开的通道集合中移除
            try {
                final File f = new File(channel.getPath()); // 获取通道对应的文件
                if (f.exists()) {
                    f.delete(); // 删除文件
                }
            } catch (Throwable ignored) {
            }
        }
    }
}
