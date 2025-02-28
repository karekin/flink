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
import org.apache.flink.core.memory.MemorySegment;

import java.io.EOFException;
import java.io.IOException;
import java.io.UTFDataFormatException;

/**
 * 所有基于多个内存页的输入视图的基类。此类包含从页面中读取数据以及检测页面边界的所有解码方法。
 * 具体的子类必须实现方法，以便在页面边界被跨越时提供下一个内存页。
 */
public abstract class AbstractPagedInputView implements DataInputView {

    private MemorySegment currentSegment; // 当前使用的内存段

    protected final int headerLength; // 每个段的起始位置需要跳过的字节数，作为段的头部

    private int positionInSegment; // 当前段中的偏移量

    private int limitInSegment; // 当前段的限制，在切换到下一个段之前，数据读取的上限

    private byte[] utfByteBuffer; // 可复用的字节缓冲区，用于 UTF-8 解码

    private char[] utfCharBuffer; // 可复用的字符缓冲区，用于 UTF-8 解码

    // --------------------------------------------------------------------------------------------

    //               Constructors (构造函数)  // --------------------------------------------------------------------------------------------

    /**
     * 创建新的视图，从指定的初始段开始读取。输入直接从初始段的头部开始，跳过指定长度的头部。
     * 如果头部长度为零，则从段的开头开始。
     * 指定的初始限制描述了在视图必须切换到下一个段之前，可以从当前段读取数据的最远位置。
     *
     * @param initialSegment 要开始读取的初始内存段。
     * @param initialLimit 初始段中最后一个有效字节后的下一个位置的索引。
     * @param headerLength 每个内存段开头需要跳过的头部字节数。所有内存段的此长度必须相同。
     */
    protected AbstractPagedInputView(MemorySegment initialSegment, int initialLimit, int headerLength) {
        this.headerLength = headerLength;
        this.positionInSegment = headerLength;
        seekInput(initialSegment, headerLength, initialLimit); // 设置初始段的读取位置和限制
    }

    /**
     * 创建一个新的视图，最初不绑定到任何内存段。此构造函数通常用于总是先进行 seek 的视图。
     * 警告：在第一次调用{@link #advance()} 或 {@link #seekInput(MemorySegment, int, int)}方法之前，视图不可读取。
     */
    protected AbstractPagedInputView(int headerLength) {
        this.headerLength = headerLength;
    }

    // --------------------------------------------------------------------------------------------

    //               Page Management (页面管理)  // --------------------------------------------------------------------------------------------

    /**
     * 获取将用于读取下一个字节的内存段。如果当前段完全耗尽，表示最后一个读取的字节是当前段中可用的最后一个字节，那么此段将不会提供下一个字节。
     * 下一个字节将通过{@link #nextSegment(MemorySegment)}方法获取。
     * @return 当前内存段。
     */
    public MemorySegment getCurrentSegment() {
        return this.currentSegment;
    }

    /**
     * 获取将读取下一个字节的位置。如果此位置等于当前限制，则下一个字节将从下一个段读取。
     * @return 下一个字节将被读取的位置。
     * @see #getCurrentSegmentLimit()
     */
    public int getCurrentPositionInSegment() {
        return this.positionInSegment;
    }

    /**
     * 获取当前内存段的限制。此值指向当前内存段中最后一个有效字节后的下一个字节的索引。
     * @return 当前内存段的限制。
     * @see #getCurrentPositionInSegment()
     */
    public int getCurrentSegmentLimit() {
        return this.limitInSegment;
    }

    /**
     * 具体子类通过此方法实现页面跨越。当当前页面耗尽，需要继续读取时，将调用此方法。如果没有更多页面可用，此方法必须抛出 {@link EOFException} 。
     * @param current 当前已读取到其限制的页面。如果是第一次调用此方法，可以为 {@code null} 。
     * @return 应该继续读取的下一个页面。不能为 {@code null} 。如果输入耗尽，应抛出 {@link EOFException} 而不是返回 {@code null} 。
     * @throws EOFException 如果没有更多段可用，则抛出此异常。
     * @throws IOException 如果由于 I/O 相关问题无法提供下一个页面，则抛出此异常。
     */
    protected abstract MemorySegment nextSegment(MemorySegment current) throws EOFException, IOException;

    /**
     * 获取指定内存段的数据读取限制。此方法应返回指定内存段中最后一个有效字节后的下一个字节的索引。当达到此位置时，视图将尝试切换到下一个内存段。
     * @param segment 要确定限制的内存段。
     * @return 指定内存段的限制。
     */
    protected abstract int getLimitForSegment(MemorySegment segment);

    /**
     * 将视图切换到下一个内存段。读取将从下一个段的头部开始。
     * 此方法使用 {@link #nextSegment(MemorySegment)} 和 {@link #getLimitForSegment(MemorySegment)} 方法来获取下一个段并设置其限制。
     * @throws IOException 如果无法获取下一个段，则抛出此异常。
     * @see #nextSegment(MemorySegment)
     * @see #getLimitForSegment(MemorySegment)
     */
    public void advance() throws IOException {
        doAdvance();
    }

    protected void doAdvance() throws IOException {
        // 注意：在 EOF 情况下，此代码确保在位置保持不变，这样 EOF 是可重复出现的（如果 nextSegment 抛出可重复的 EOFException）
        this.currentSegment = nextSegment(this.currentSegment); // 获取下一个内存段
        this.limitInSegment = getLimitForSegment(this.currentSegment); // 获取下一个段的限制
        this.positionInSegment = this.headerLength; // 从下一个段的头部开始读取
    }

    /** @return header length. (返回头部长度。) */
    public int getHeaderLength() {
        return headerLength;
    }

    /**
     * 设置视图的内部状态，使得下一个字节将从指定的内存段的指定位置开始读取。指定的内存段将提供字节直到指定的限制位置。
     * @param segment 将从当前段读取的下一字节的内存段。
     * @param positionInSegment 该段中开始读取的偏移量（起始位置）。
     * @param limitInSegment 该段的限制。到达此位置后，视图将尝试切换到下一个段。
     */
    protected void seekInput(MemorySegment segment, int positionInSegment, int limitInSegment) {
        this.currentSegment = segment;
        this.positionInSegment = positionInSegment;
        this.limitInSegment = limitInSegment;
    }

    /**
     *清除视图的内部状态。调用此方法后，所有读取尝试都会失败，直到调用 {@link #advance()} 或 {@link #seekInput(MemorySegment, int, int)} 方法。  */
    protected void clear() {
        this.currentSegment = null;
        this.positionInSegment = this.headerLength;
        this.limitInSegment = headerLength;
    }

    // --------------------------------------------------------------------------------------------

    //               Data Input Specific methods (特定于数据输入的方法)  // --------------------------------------------------------------------------------------------

    @Override
    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length); // 将字节数组的读取委托给重载方法
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (off < 0 || len < 0 || off + len > b.length) {
            throw new IndexOutOfBoundsException(); // 参数错误检查
        }

        int remaining = this.limitInSegment - this.positionInSegment; // 当前段中剩余的可读字节数

        if (remaining >= len) {
            this.currentSegment.get(this.positionInSegment, b, off, len); // 一次性读取所有所需字节
            this.positionInSegment += len; // 更新偏移量
            return len;
        } else {
            if (remaining == 0) {
                try {
                    advance(); // 当前段已无数据，切换到下一个段
                } catch (EOFException eof) {
                    return -1; // 返回 -1 表示无更多数据
                }
                remaining = this.limitInSegment - this.positionInSegment; // 更新剩余字节数
            }

            int bytesRead = 0; // 已读取的字节数
            while (true) {
                int toRead = Math.min(remaining, len - bytesRead); // 当前段中要读取的字节数
                this.currentSegment.get(this.positionInSegment, b, off, toRead); // 读取字节
                off += toRead; // 更新目标数组的偏移量
                bytesRead += toRead; // 更新已读取字节数
                if (len > bytesRead) { // 如果还需要读取更多字节
                    try {
                        advance(); // 切换到下一个段
                    } catch (EOFException eof) {
                        this.positionInSegment += toRead; // 将当前段中的偏移量更新为已读取的部分
                        return bytesRead; // 返回已读取的部分字节数
                    }
                    remaining = this.limitInSegment - this.positionInSegment; // 更新剩余字节数
                } else {
                    this.positionInSegment += toRead; // 更新当前段中的偏移量
                    break; // 退出循环
                }
            }
            return len; // 返回读取的所有字节
        }
    }

    @Override
    public void readFully(byte[] b) throws IOException {
        readFully(b, 0, b.length); // 委托给重载方法
    }

    @Override
    public void readFully(byte[] b, int off, int len) throws IOException {
        int bytesRead = read(b, off, len); // 读取字节
        if (bytesRead < len) {
            throw new EOFException("输入视图中没有足够的数据。"); // 如果读取的字节数不足，抛出异常
        }
    }

    @Override
    public boolean readBoolean() throws IOException {
        return readByte() == 1; // 读取字节并转换为布尔值
    }

    @Override
    public byte readByte() throws IOException {
        if (this.positionInSegment < this.limitInSegment) { // 如果当前段中有剩余数据
            return this.currentSegment.get(this.positionInSegment++); // 直接从当前段读取字节并更新偏移量
        } else { // 当前段已无数据，切换到下一个段并重新读取
            advance();
            return readByte();
        }
    }

    @Override
    public int readUnsignedByte() throws IOException {
        return readByte() & 0xff; // 读取无符号字节
    }

    @Override
    public short readShort() throws IOException {
        if (this.positionInSegment < this.limitInSegment - 1) { // 当前段中数据充足，可一次性读取 short 值
            final short v = this.currentSegment.getShortBigEndian(this.positionInSegment);
            this.positionInSegment += 2; // 更新偏移量
            return v;
        } else if (this.positionInSegment == this.limitInSegment) { // 当前段已无数据，切换到下一个段并重新读取
            advance();
            return readShort();
        } else { // 当前段中数据不足，跨段读取
            return (short) ((readUnsignedByte() << 8) | readUnsignedByte()); // 手动组合两个字节为 short 值
        }
    }

    @Override
    public int readUnsignedShort() throws IOException {
        if (this.positionInSegment < this.limitInSegment - 1) { // 当前段中数据充足，可一次性读取 unsigned short 值
            final int v = this.currentSegment.getShortBigEndian(this.positionInSegment) & 0xffff;
            this.positionInSegment += 2; // 更新偏移量
            return v;
        } else if (this.positionInSegment == this.limitInSegment) { // 当前段已无数据，切换到下一个段并重新读取
            advance();
            return readUnsignedShort();
        } else { // 当前段中数据不足，跨段读取
            return (readUnsignedByte() << 8) | readUnsignedByte(); // 手动组合两个字节为 unsigned short 值
        }
    }

    @Override
    public char readChar() throws IOException {
        if (this.positionInSegment < this.limitInSegment - 1) { // 当前段中数据充足，可一次性读取 char 值
            final char v = this.currentSegment.getCharBigEndian(this.positionInSegment);
            this.positionInSegment += 2; // 更新偏移量
            return v;
        } else if (this.positionInSegment == this.limitInSegment) { // 当前段已无数据，切换到下一个段并重新读取
            advance();
            return readChar();
        } else { // 当前段中数据不足，跨段读取
            return (char) ((readUnsignedByte() << 8) | readUnsignedByte()); // 手动组合两个字节为 char 值
        }
    }

    @Override
    public int readInt() throws IOException {
        if (this.positionInSegment < this.limitInSegment - 3) { // 当前段中数据充足，可一次性读取 int 值
            final int v = this.currentSegment.getIntBigEndian(this.positionInSegment);
            this.positionInSegment += 4; // 更新偏移量
            return v;
        } else if (this.positionInSegment == this.limitInSegment) { // 当前段已无数据，切换到下一个段并重新读取
            advance();
            return readInt();
        } else { // 当前段中数据不足，跨段读取
            return (readUnsignedByte() << 24) | (readUnsignedByte() << 16) | (readUnsignedByte() << 8) | readUnsignedByte(); // 手动组合四个字节为 int 值
        }
    }

    @Override
    public long readLong() throws IOException {
        if (this.positionInSegment < this.limitInSegment - 7) { // 当前段中数据充足，可一次性读取 long 值
            final long v = this.currentSegment.getLongBigEndian(this.positionInSegment);
            this.positionInSegment += 8; // 更新偏移量
            return v;
        } else if (this.positionInSegment == this.limitInSegment) { // 当前段已无数据，切换到下一个段并重新读取
            advance();
            return readLong();
        } else { // 当前段中数据不足，跨段读取
            long l = 0L;
            l |= ((long) readUnsignedByte()) << 56; // 组合第一个字节到最高位
            l |= ((long) readUnsignedByte()) << 48;
            l |= ((long) readUnsignedByte()) << 40;
            l |= ((long) readUnsignedByte()) << 32;
            l |= ((long) readUnsignedByte()) << 24;
            l |= ((long) readUnsignedByte()) << 16;
            l |= ((long) readUnsignedByte()) << 8;
            l |= (long) readUnsignedByte(); // 组合最后一个字节到最低位
            return l;
        }
    }

    @Override
    public float readFloat() throws IOException {
        return Float.intBitsToFloat(readInt()); // 将四位字节组成的整数转换为浮点数
    }

    @Override
    public double readDouble() throws IOException {
        return Double.longBitsToDouble(readLong()); // 将八位字节组成的长整数转换为双精度浮点数
    }

    @Override
    public String readLine() throws IOException {
        final StringBuilder bld = new StringBuilder(32); // 使用StringBuilder构建字符串

        try {
            int b;
            while ((b = readUnsignedByte()) != '\n') { // 循环读取字符直到换行符
                if (b != '\r') { // 跳过回车符
                    bld.append((char) b);
                }
            }
        } catch (EOFException eofex) {
            // 忽略EOF异常，返回已构建的字符串（如果有）
        }

        if (bld.length() == 0) {
            return null; // 如果字符串为空，返回 null
        }

        // 去除末尾的回车符（如果有的话）
        int len = bld.length();
        if (len > 0 && bld.charAt(len - 1) == '\r') {
            bld.setLength(len - 1);
        }
        return bld.toString();
    }

    @Override
    public String readUTF() throws IOException {
        final int utflen = readUnsignedShort(); // 读取UTF字符串的长度

        final byte[] bytearr;
        final char[] chararr;

        // 动态调整字节缓冲区和字符缓冲区的大小，以避免不必要的内存使用
        if (this.utfByteBuffer == null || this.utfByteBuffer.length < utflen) {
            bytearr = new byte[utflen];
            this.utfByteBuffer = bytearr;
        } else {
            bytearr = this.utfByteBuffer;
        }
        if (this.utfCharBuffer == null || this.utfCharBuffer.length < utflen) {
            chararr = new char[utflen];
            this.utfCharBuffer = chararr;
        } else {
            chararr = this.utfCharBuffer;
        }

        int c, char2, char3;
        int count = 0; // 字节数组中的当前读取位置
        int chararrCount = 0; // 字符数组中的当前写入位置

        readFully(bytearr, 0, utflen); // 将字节读取到缓冲区

        // 解码 UTF-8 字节数组为字符串
        while (count < utflen) {
            c = (int) bytearr[count] & 0xff;
            if (c > 127) {
                break; // 如果最高位为1，表示多字节字符，退出循环，进入多字节处理逻辑
            }
            count++; // 单字节字符，直接添加到字符串
            chararr[chararrCount++] = (char) c;
        }

        // 多字节字符处理部分
        while (count < utflen) {
            c = (int) bytearr[count] & 0xff;
            switch (c >> 4) { // 检查最高四位，确定字符编码方式
                case 0:
                case 1:
                case 2:
                case 3:
                case 4:
                case 5:
                case 6:
                case 7: // 0xxxxxxx，单字节字符
                    count++;
                    chararr[chararrCount++] = (char) c;
                    break;
                case 12:
                case 13: // 110x xxxx，双字节字符
                    count += 2;
                    if (count > utflen) { // 字节不完整，抛出异常
                        throw new UTFDataFormatException("输入数据格式错误：未完成的字符位于末尾");
                    }
                    char2 = (int) bytearr[count - 1];
                    if ((char2 & 0xC0) != 0x80) { // 检查后续字节是否为10xxxxxx形式
                        throw new UTFDataFormatException("输入数据格式错误：无效的字符编码，位置：" + count);
                    }
                    chararr[chararrCount++] = (char) (((c & 0x1F) << 6) | (char2 & 0x3F)); // 组合两个字节为字符
                    break;
                case 14: // 1110 xxxx，三字节字符
                    count += 3;
                    if (count > utflen) { // 字节不完整，抛出异常
                        throw new UTFDataFormatException("输入数据格式错误：未完成的字符位于末尾");
                    }
                    char2 = (int) bytearr[count - 2];
                    char3 = (int) bytearr[count - 1];
                    if (((char2 & 0xC0) != 0x80) || ((char3 & 0xC0) != 0x80)) { // 检查后续字节是否为10xxxxxx形式
                        throw new UTFDataFormatException("输入数据格式错误：无效的字符编码，位置：" + (count - 1));
                    }
                    chararr[chararrCount++] = (char) (((c & 0x0F) << 12) | ((char2 & 0x3F) << 6) | ((char3 & 0x3F) << 0)); // 组合三个字节为字符
                    break;
                default: // 其他情况视为无效字符编码
                    throw new UTFDataFormatException("输入数据格式错误：无效的字符编码，位置：" + count);
            }
        }
        return new String(chararr, 0, chararrCount); // 返回解码后的字符串
    }

    @Override
    public int skipBytes(int n) throws IOException {
        if (n < 0) {
            throw new IllegalArgumentException(); // 跳过的字节数不能为负数
        }

        int remaining = this.limitInSegment - this.positionInSegment; // 当前段中剩余的可跳过字节数

        if (remaining >= n) {
            this.positionInSegment += n; // 直接跳过所需的字节数
            return n;
        } else {
            if (remaining == 0) { // 当前段已无数据，切换到下一个段
                try {
                    advance();
                } catch (EOFException eofex) {
                    return 0; // 返回跳过的字节数为0，表示已到达末尾
                }
                remaining = this.limitInSegment - this.positionInSegment; // 更新剩余字节数
            }

            int skipped = 0; // 已跳过的字节数
            while (true) {
                int toSkip = Math.min(remaining, n); // 当前段中要跳过的字节数
                n -= toSkip;
                skipped += toSkip;

                if (n > 0) { // 如果仍有需要跳过的字节
                    try {
                        advance(); // 切换到下一个段
                    } catch (EOFException eofex) {
                        return skipped; // 返回已跳过的字节数
                    }
                    remaining = this.limitInSegment - this.positionInSegment; // 更新剩余字节数
                } else {
                    this.positionInSegment += toSkip; // 更新当前段的偏移量
                    break; // 退出循环
                }
            }
            return skipped; // 返回跳过的字节数
        }
    }

    @Override
    public void skipBytesToRead(int numBytes) throws IOException {
        if (numBytes < 0) {
            throw new IllegalArgumentException(); // 参数错误检查
        }

        int remaining = this.limitInSegment - this.positionInSegment; // 当前段中剩余的可用字节数

        if (remaining >= numBytes) { // 当前段中的字节数足够跳过
            this.positionInSegment += numBytes; // 更新偏移量
        } else {
            if (remaining == 0) { // 当前段已无数据，切换到下一个段
                advance();
                remaining = this.limitInSegment - this.positionInSegment; // 更新剩余字节数
            }

            while (true) { // 循环跳过多个段中的字节
                if (numBytes > remaining) { // 需要跨段跳过
                    numBytes -= remaining; // 计算剩余需要跳过的字节数
                    advance(); // 切换到下一个段
                    remaining = this.limitInSegment - this.positionInSegment; // 更新剩余字节数
                } else { // 当前段的剩余字节数足够跳过
                    this.positionInSegment += numBytes; // 更新偏移量
                    break; // 退出循环
                }
            }
        }
    }
}
