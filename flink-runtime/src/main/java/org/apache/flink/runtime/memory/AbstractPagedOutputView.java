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

package org.apache.flink.runtime.memory;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentWritable;

import java.io.IOException;
import java.io.UTFDataFormatException;

/**
 * 所有基于多个内存页的输出视图的基类。此类包含将数据写入页面以及检测页面边界的编码方法。具体的子类必须实现方法，以便在页面边界被跨越时收集当前内存页并提供下一个内存页。  * @授课老师: 码界探索  * @微信: 252810631  * @版权所有: 请尊重劳动成果  */
public abstract class AbstractPagedOutputView implements DataOutputView, MemorySegmentWritable {

    private MemorySegment currentSegment; // 当前正在写入的内存段
    protected final int segmentSize; // 内存段的大小
    protected final int headerLength; // 每个内存段开头需要跳过的头部字节数
    private int positionInSegment; // 当前段中的偏移量
    private byte[] utfBuffer; // 用于 UTF 编码的可复用字节数组

    // --------------------------------------------------------------------------------------------
    //                                    Constructors (构造方法)
    // --------------------------------------------------------------------------------------------

    /**
     * 创建新的输出视图，初始写入到指定的初始内存段。此视图中的所有内存段的大小必须为给定的 {@code segmentSize}。每个段的开头会保留长度为 {@code headerLength} 的头部。
     * @param initialSegment 视图开始写入的初始内存段
     * @param segmentSize 内存段的大小
     * @param headerLength 每个内存段开头需要跳过的头部字节数
     */
    protected AbstractPagedOutputView(MemorySegment initialSegment, int segmentSize, int headerLength) {
        if (initialSegment == null) {
            throw new NullPointerException("初始内存段不能为空");
        }
        this.segmentSize = segmentSize;
        this.headerLength = headerLength;
        this.currentSegment = initialSegment;
        this.positionInSegment = headerLength;
    }

    /**
     * 初始化分页输出视图的构造函数，设置内存段大小和头部长度。
     * @param segmentSize 内存段的大小
     * @param headerLength 每个内存段开头需要跳过的头部字节数
     */
    protected AbstractPagedOutputView(int segmentSize, int headerLength) {
        this.segmentSize = segmentSize;
        this.headerLength = headerLength;
    }

    // --------------------------------------------------------------------------------------------
    //                                  Page Management (页面管理)
    // --------------------------------------------------------------------------------------------

    /**
     * 必须返回一个内存段。如果无法提供更多内存段，必须抛出 {@link java.io.EOFException}。
     * @param current 当前内存段
     * @param positionInCurrent 当前内存段中的位置，位于最后一个有效字节的后一个位置
     * @return 下一个内存段
     * @throws IOException 如果当前段无法处理或无法获取新段
     */
    protected abstract MemorySegment nextSegment(MemorySegment current, int positionInCurrent) throws IOException;

    /**
     * 获取当前输出视图正在写入的内存段。
     * @return 正在写入的内存段
     */
    public MemorySegment getCurrentSegment() {
        return this.currentSegment;
    }

    /**
     * 获取当前内存段中的当前写入位置（将要写入下一个字节的位置）。
     * @return 当前内存段中的写入偏移量
     */
    public int getCurrentPositionInSegment() {
        return this.positionInSegment;
    }

    /**
     * 获取视图使用的内存段的大小。
     * @return 内存段的大小
     */
    public int getSegmentSize() {
        return this.segmentSize;
    }

    /**
     * 将输出视图切换到下一页。此方法通过调用 {@link #nextSegment(MemorySegment, int)} 方法，将当前内存段传递给具体子类的实现，并获取下一个将要写入的内存段。写入将在新段的头部之后继续进行。
     * @throws IOException 如果无法处理当前段或无法获取新段
     */
    public void advance() throws IOException {
        this.currentSegment = nextSegment(this.currentSegment, this.positionInSegment); // 切换到下一个内存段
        this.positionInSegment = this.headerLength; // 从新段的头部开始写入
    }

    /** @return header length. (返回头部长度。) */
    public int getHeaderLength() {
        return headerLength;
    }

    /**
     * 设置输出视图的内部状态为指定的内存段和段内的指定位置。
     * @param seg 将要写入下一个字节的内存段
     * @param position 内存段中开始写入下一个字节的偏移量
     */
    protected void seekOutput(MemorySegment seg, int position) {
        this.currentSegment = seg;
        this.positionInSegment = position;
    }

    /**
     * 清除内部状态。在调用 {@link #advance()} 或 {@link #seekOutput(MemorySegment, int)} 之前，后续的写操作都将失败。
     * @see #advance()
     * @see #seekOutput(MemorySegment, int)
     */
    protected void clear() {
        this.currentSegment = null;
        this.positionInSegment = this.headerLength;
    }

    // --------------------------------------------------------------------------------------------
    //                               Data Output Specific methods (特定于数据输出的方法)
    // --------------------------------------------------------------------------------------------

    @Override
    public void write(int b) throws IOException {
        writeByte(b); // 将整数参数作为单个字节写入
    }

    @Override
    public void write(byte[] b) throws IOException {
        write(b, 0, b.length); // 将字节数组的全部内容写入
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        int remaining = this.segmentSize - this.positionInSegment; // 当前段中剩余的可写字节数

        if (remaining >= len) {
            this.currentSegment.put(this.positionInSegment, b, off, len); // 直接写入当前段
            this.positionInSegment += len; // 更新偏移量
        } else {
            if (remaining == 0) { // 当前段已满，切换到下一个段
                advance();
                remaining = this.segmentSize - this.positionInSegment; // 更新剩余字节数
            }
            while (true) { // 循环处理分页写入
                int toPut = Math.min(remaining, len); // 当前段中要写入的字节数
                this.currentSegment.put(this.positionInSegment, b, off, toPut); // 写入字节
                off += toPut; // 更新源数组的偏移量
                len -= toPut; // 更新剩余字节数

                if (len > 0) { // 如果还有更多字节需要写入
                    this.positionInSegment = this.segmentSize; // 当前段已满
                    advance(); // 切换到下一个段
                    remaining = this.segmentSize - this.positionInSegment; // 更新剩余字节数
                } else {
                    this.positionInSegment += toPut; // 更新当前段的偏移量
                    break; // 跳出循环
                }
            }
        }
    }

    @Override
    public void writeBoolean(boolean v) throws IOException {
        writeByte(v ? 1 : 0); // 将布尔值转换为字节（1 或 0）写入
    }

    @Override
    public void writeByte(int v) throws IOException {
        if (this.positionInSegment < this.segmentSize) { // 当前段有剩余空间
            this.currentSegment.put(this.positionInSegment++, (byte) v); // 写入字节并更新偏移量
        } else { // 当前段已满，切换到下一个段
            advance();
            writeByte(v);
        }
    }

    @Override
    public void writeShort(int v) throws IOException {
        if (this.positionInSegment < this.segmentSize - 1) { // 当前段中剩余空间足够写入两个字节
            this.currentSegment.putShortBigEndian(this.positionInSegment, (short) v); // 写入 short 值
            this.positionInSegment += 2; // 更新偏移量
        } else if (this.positionInSegment == this.segmentSize) { // 当前段已满，切换到下一个段
            advance();
            writeShort(v);
        } else { // 跨段写入
            writeByte(v >> 8); // 写入高字节
            writeByte(v); // 写入低字节
        }
    }

    @Override
    public void writeChar(int v) throws IOException {
        if (this.positionInSegment < this.segmentSize - 1) { // 当前段中剩余空间足够写入两个字节
            this.currentSegment.putCharBigEndian(this.positionInSegment, (char) v); // 写入 char 值
            this.positionInSegment += 2; // 更新偏移量
        } else if (this.positionInSegment == this.segmentSize) { // 当前段已满，切换到下一个段
            advance();
            writeChar(v);
        } else { // 跨段写入
            writeByte(v >> 8); // 写入高字节
            writeByte(v); // 写入低字节
        }
    }

    @Override
    public void writeInt(int v) throws IOException {
        if (this.positionInSegment < this.segmentSize - 3) { // 当前段中剩余空间足够写入四个字节
            this.currentSegment.putIntBigEndian(this.positionInSegment, v); // 写入 int 值
            this.positionInSegment += 4; // 更新偏移量
        } else if (this.positionInSegment == this.segmentSize) { // 当前段已满，切换到下一个段
            advance();
            writeInt(v);
        } else { // 跨段写入
            writeByte(v >> 24); // 写入最高字节
            writeByte(v >> 16); // 写入次高字节
            writeByte(v >> 8); // 写入次低字节
            writeByte(v); // 写入最低字节
        }
    }

    @Override
    public void writeLong(long v) throws IOException {
        if (this.positionInSegment < this.segmentSize - 7) { // 当前段中剩余空间足够写入八个字节
            this.currentSegment.putLongBigEndian(this.positionInSegment, v); // 写入 long 值
            this.positionInSegment += 8; // 更新偏移量
        } else if (this.positionInSegment == this.segmentSize) { // 当前段已满，切换到下一个段
            advance();
            writeLong(v);
        } else { // 跨段写入
            writeByte((int) (v >> 56)); // 写入最高字节
            writeByte((int) (v >> 48)); // 写入下一个字节
            writeByte((int) (v >> 40)); // 写入下一个字节
            writeByte((int) (v >> 32)); // 写入下一个字节
            writeByte((int) (v >> 24)); // 写入下一个字节
            writeByte((int) (v >> 16)); // 写入下一个字节
            writeByte((int) (v >> 8)); // 写入下一个字节
            writeByte((int) v); // 写入最低字节
        }
    }

    @Override
    public void writeFloat(float v) throws IOException {
        writeInt(Float.floatToRawIntBits(v)); // 将浮点数转换为整数并写入
    }

    @Override
    public void writeDouble(double v) throws IOException {
        writeLong(Double.doubleToRawLongBits(v)); // 将双精度浮点数转换为长整数并写入
    }

    @Override
    public void writeBytes(String s) throws IOException {
        for (int i = 0; i < s.length(); i++) {
            writeByte(s.charAt(i)); // 将字符串的每个字符作为字节写入
        }
    }

    @Override
    public void writeChars(String s) throws IOException {
        for (int i = 0; i < s.length(); i++) {
            writeChar(s.charAt(i)); // 将字符串的每个字符作为字符写入
        }
    }

    @Override
    public void writeUTF(String str) throws IOException {
        int strlen = str.length();
        int utflen = 0;
        int c, count = 0;

        // 遍历字符串，计算其 UTF-8 编码所需的字节数
        for (int i = 0; i < strlen; i++) {
            c = str.charAt(i);
            if ((c >= 0x0001) && (c <= 0x007F)) { // 单字节字符
                utflen++;
            } else if (c > 0x07FF) { // 三字节字符
                utflen += 3;
            } else { // 双字节字符
                utflen += 2;
            }
        }

        // 检查字符串长度是否超过最大限制
        if (utflen > 65535) {
            throw new UTFDataFormatException("编码后的字符串过长: " + utflen);
        }

        // 根据需要调整字节数组的大小
        if (this.utfBuffer == null || this.utfBuffer.length < utflen + 2) {
            this.utfBuffer = new byte[utflen + 2];
        }
        final byte[] bytearr = this.utfBuffer;

        // 将字节数长度写入字节数组
        bytearr[count++] = (byte) ((utflen >>> 8) & 0xFF);
        bytearr[count++] = (byte) (utflen & 0xFF);

        // 将字符串编码为 UTF-8 字节数组
        int i;
        for (i = 0; i < strlen; i++) {
            c = str.charAt(i);
            if (!((c >= 0x0001) && (c <= 0x007F))) { // 非单字节字符
                break;
            }
            bytearr[count++] = (byte) c; // 单字节字符直接写入
        }

        for (; i < strlen; i++) {
            c = str.charAt(i);
            if ((c >= 0x0001) && (c <= 0x007F)) { // 单字节字符直接写入
                bytearr[count++] = (byte) c;
            } else if (c > 0x07FF) { // 三字节字符
                bytearr[count++] = (byte) (0xE0 | ((c >> 12) & 0x0F));
                bytearr[count++] = (byte) (0x80 | ((c >> 6) & 0x3F));
                bytearr[count++] = (byte) (0x80 | (c & 0x3F));
            } else { // 双字节字符
                bytearr[count++] = (byte) (0xC0 | ((c >> 6) & 0x1F));
                bytearr[count++] = (byte) (0x80 | (c & 0x3F));
            }
        }

        write(bytearr, 0, utflen + 2); // 将 UTF-8 编码后的字节数组写入
    }

    @Override
    public void skipBytesToWrite(int numBytes) throws IOException {
        while (numBytes > 0) {
            final int remaining = this.segmentSize - this.positionInSegment; // 当前段中剩余的字节数
            if (numBytes <= remaining) {
                this.positionInSegment += numBytes; // 跳过指定字节数
                return;
            }
            this.positionInSegment = this.segmentSize; // 当前段已满
            advance(); // 切换到下一个段
            numBytes -= remaining; // 减少剩余需要跳过的字节数
        }
    }

    @Override
    public void write(DataInputView source, int numBytes) throws IOException {
        while (numBytes > 0) {
            final int remaining = this.segmentSize - this.positionInSegment; // 当前段中剩余的字节数
            if (numBytes <= remaining) {
                this.currentSegment.put(source, this.positionInSegment, numBytes); // 从源读取并写入当前段
                this.positionInSegment += numBytes; // 更新偏移量
                return;
            }

            if (remaining > 0) {
                this.currentSegment.put(source, this.positionInSegment, remaining); // 从源读取并写入当前段
                this.positionInSegment = this.segmentSize; // 当前段已满
                numBytes -= remaining; // 减少剩余需要写入的字节数
            }

            advance(); // 切换到下一个段
        }
    }

    @Override
    public void write(MemorySegment segment, int off, int len) throws IOException {
        int remaining = this.segmentSize - this.positionInSegment; // 当前段中剩余的字节数

        if (remaining >= len) {
            segment.copyTo(off, currentSegment, positionInSegment, len); // 直接复制内存段中的数据
            this.positionInSegment += len; // 更新偏移量
        } else {
            if (remaining == 0) { // 当前段已满，切换到下一个段
                advance();
                remaining = this.segmentSize - this.positionInSegment; // 更新剩余字节数
            }
            while (true) { // 循环处理跨段写入
                int toPut = Math.min(remaining, len); // 当前段中要写入的字节数
                segment.copyTo(off, currentSegment, positionInSegment, toPut); // 写入字节
                off += toPut; // 更新源内存段的偏移量
                len -= toPut; // 更新剩余字节数

                if (len > 0) { // 如果还有更多字节需要写入
                    this.positionInSegment = this.segmentSize; // 当前段已满
                    advance(); // 切换到下一个段
                    remaining = this.segmentSize - this.positionInSegment; // 更新剩余字节数
                } else {
                    this.positionInSegment += toPut; // 更新当前段的偏移量
                    break; // 跳出循环
                }
            }
        }
    }
}
