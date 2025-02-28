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

import org.apache.flink.runtime.io.compression.BlockCompressionFactory;
import org.apache.flink.runtime.io.disk.iomanager.AbstractChannelReaderInputView;
import org.apache.flink.runtime.io.disk.iomanager.AbstractChannelWriterOutputView;
import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.memory.AbstractPagedOutputView;
import org.apache.flink.table.runtime.io.ChannelWithMeta;
import org.apache.flink.table.runtime.util.FileChannelUtil;
import org.apache.flink.util.MutableObjectIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Flink的外部排序器的溢出文件合并器。它最多将maxFanIn个溢出文件合并一次。
 *
 * @param <Entry> 要归并排序的条目类型。
 */
public abstract class AbstractBinaryExternalMerger<Entry> implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractBinaryExternalMerger.class);

    private volatile boolean closed;

    private final int maxFanIn; // 最大合并扇入数
    private final SpillChannelManager channelManager; // 溢出通道管理器
    private final boolean compressionEnabled; // 是否启用压缩
    private final BlockCompressionFactory compressionCodecFactory; // 压缩解压工厂
    private final int compressionBlockSize; // 压缩块大小

    protected final int pageSize; // 页面大小
    protected final IOManager ioManager; // 输入输出管理器

    public AbstractBinaryExternalMerger(
            IOManager ioManager,
            int pageSize,
            int maxFanIn,
            SpillChannelManager channelManager,
            boolean compressionEnabled,
            BlockCompressionFactory compressionCodecFactory,
            int compressionBlockSize) {
        this.ioManager = ioManager; // 初始化输入输出管理器
        this.pageSize = pageSize; // 初始化页面大小
        this.maxFanIn = maxFanIn; // 初始化最大合并扇入数
        this.channelManager = channelManager; // 初始化通道管理器
        this.compressionEnabled = compressionEnabled; // 初始化是否启用压缩
        this.compressionCodecFactory = compressionCodecFactory; // 初始化压缩解压工厂
        this.compressionBlockSize = compressionBlockSize; // 初始化压缩块大小
    }

    @Override
    public void close() {
        this.closed = true; // 标记已关闭
    }

    /**
     * 返回一个迭代器，该迭代器遍历所有给定通道合并后的结果。
     *
     * @param channelIDs 需要合并并返回的通道。
     * @return 输入通道记录的合并迭代器。
     * @throws IOException 如果读取器遇到 I/O 问题。
     */
    public BinaryMergeIterator<Entry> getMergingIterator(
            List<ChannelWithMeta> channelIDs, List<FileIOChannel> openChannels) throws IOException {
        // 为每个通道 ID 创建一个迭代器
        if (LOG.isDebugEnabled()) {
            LOG.debug("正在合并 " + channelIDs.size() + " 个已排序的流。");
        }

        final List<MutableObjectIterator<Entry>> iterators = new ArrayList<>(channelIDs.size() + 1);

        for (ChannelWithMeta channel : channelIDs) {
            AbstractChannelReaderInputView view =
                    FileChannelUtil.createInputView(
                            ioManager, // 输入输出管理器
                            channel, // 当前通道
                            openChannels, // 已打开的通道列表
                            compressionEnabled, // 是否启用压缩
                            compressionCodecFactory, // 压缩解压工厂
                            compressionBlockSize, // 压缩块大小
                            pageSize // 页面大小
                    );
            iterators.add(channelReaderInputViewIterator(view)); // 添加输入视图的条目迭代器
        }

        return new BinaryMergeIterator<>( // 返回合并迭代器
                iterators, // 输入迭代器列表
                mergeReusedEntries(channelIDs.size()), // 合并复用的条目
                mergeComparator() // 合并比较器
        );
    }

    /**
     * 将给定的已排序运行合并为数量较少的已排序运行。
     *
     * @param channelIDs 需要合并的已排序运行的通道 ID。
     * @return 合并通道的 ID 列表。
     * @throws IOException 如果读取器或写入器遇到 I/O 问题。
     */
    public List<ChannelWithMeta> mergeChannelList(List<ChannelWithMeta> channelIDs)
            throws IOException {
        // 一个长度为 maxFanIn^i 的通道列表可以经过 i-1 轮完整合并，每轮合并有 maxFanIn 个输入通道。
        // 不完整的合并轮次包括少于 maxFanIn 个输入通道的合并。最有效的是先进行不完整的合并轮次。
        final double scale = Math.ceil(Math.log(channelIDs.size()) / Math.log(maxFanIn)) - 1;

        final int numStart = channelIDs.size(); // 初始通道数量
        final int numEnd = (int) Math.pow(maxFanIn, scale); // 结果通道数量

        final int numMerges = (int) Math.ceil((numStart - numEnd) / (double) (maxFanIn - 1)); // 合并次数

        final int numNotMerged = numEnd - numMerges; // 不合并的通道数量
        final int numToMerge = numStart - numNotMerged; // 需要合并的通道数量

        // 未合并的通道 ID 直接复制到结果列表中
        final List<ChannelWithMeta> mergedChannelIDs = new ArrayList<>(numEnd);
        mergedChannelIDs.addAll(channelIDs.subList(0, numNotMerged)); // 添加未合并的通道

        final int channelsToMergePerStep = (int) Math.ceil(numToMerge / (double) numMerges); // 每次合并的通道数量

        final List<ChannelWithMeta> channelsToMergeThisStep =
                new ArrayList<>(channelsToMergePerStep); // 当前步骤要合并的通道
        int channelNum = numNotMerged; // 当前通道索引
        while (!closed && channelNum < channelIDs.size()) {
            channelsToMergeThisStep.clear(); // 清空当前步骤要合并的通道列表

            // 添加通道到当前步骤合并列表
            for (int i = 0; i < channelsToMergePerStep && channelNum < channelIDs.size(); i++, channelNum++) {
                channelsToMergeThisStep.add(channelIDs.get(channelNum)); // 添加通道
            }

            mergedChannelIDs.add(mergeChannels(channelsToMergeThisStep)); // 合并通道并添加到结果列表
        }

        return mergedChannelIDs; // 返回合并后的通道列表
    }

    /**
     * 将给定通道 ID 描述的已排序运行合并为一个已排序运行。
     *
     * @param channelIDs 运行通道的 ID。
     * @return 描述合并运行的通道的 ID 和块数。
     */
    private ChannelWithMeta mergeChannels(List<ChannelWithMeta> channelIDs) throws IOException {
        // 目标迭代器的列表
        List<FileIOChannel> openChannels = new ArrayList<>(channelIDs.size()); // 已打开的通道列表

        BinaryMergeIterator<Entry> mergeIterator = getMergingIterator(channelIDs, openChannels); // 获取合并迭代器

        // 创建一个新的通道写入器
        final FileIOChannel.ID mergedChannelID = ioManager.createChannel(); // 创建新通道
        channelManager.addChannel(mergedChannelID); // 添加通道到通道管理器

        AbstractChannelWriterOutputView output = null; // 输出视图
        int numBytesInLastBlock = 0; // 最后一块的字节数
        int numBlocksWritten = 0; // 写入的块数

        try {
            output = FileChannelUtil.createOutputView( // 创建输出视图
                    ioManager, // 输入输出管理器
                    mergedChannelID, // 合并后的通道 ID
                    compressionEnabled, // 是否启用压缩
                    compressionCodecFactory, // 压缩解压工厂
                    compressionBlockSize, // 压缩块大小
                    pageSize // 页面大小
            );

            writeMergingOutput(mergeIterator, output); // 将合并后的数据写入输出视图
            numBytesInLastBlock = output.close(); // 关闭输出视图并获取最后的字节数
            numBlocksWritten = output.getBlockCount(); // 获取写入的块数
        } catch (IOException e) {
            if (output != null) { // 如果输出视图不为空，关闭并删除通道
                output.close();
                output.getChannel().deleteChannel();
            }
            throw e; // 抛出异常
        }

        // 删除并关闭旧通道
        for (FileIOChannel channel : openChannels) {
            channelManager.removeChannel(channel.getChannelID()); // 从通道管理器中移除通道
            try {
                channel.closeAndDelete(); // 关闭并删除通道
            } catch (Throwable ignored) {
            }
        }

        return new ChannelWithMeta(mergedChannelID, numBlocksWritten, numBytesInLastBlock); // 返回合并后的通道信息
    }

    // -------------------------------------------------------------------------------------------

    /**
     * 返回从输入视图读取的条目迭代器。
     */
    protected abstract MutableObjectIterator<Entry> channelReaderInputViewIterator(
            AbstractChannelReaderInputView inView);

    /**
     * 返回在合并过程中使用的比较器。
     */
    protected abstract Comparator<Entry> mergeComparator();

    /**
     * 返回在合并过程中复用的条目对象。
     */
    protected abstract List<Entry> mergeReusedEntries(int size);

    /**
     * 从合并后的流中读取数据并写入输出。
     */
    protected abstract void writeMergingOutput(
            MutableObjectIterator<Entry> mergeIterator, AbstractPagedOutputView output)
            throws IOException;
}
