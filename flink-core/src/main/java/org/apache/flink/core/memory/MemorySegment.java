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

package org.apache.flink.core.memory;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.ReadOnlyBufferException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.apache.flink.core.memory.MemoryUtils.getByteBufferAddress;

/**
 * 这个类表示一个由 Flink 管理的内存块。
 *
 * <p>内存可以位于堆内、堆外直接内存或者堆外非直接内存。这个类透明地处理这些内存类型。
 *
 * <p>这个类的概念类似于 Java 的 {@link java.nio.ByteBuffer}，但添加了额外的二进制比较、交换和复制方法。
 * 它还使用折叠的范围检查和内存段释放检查，提供了绝对定位的大批量读写方法，保证了线程安全。
 * 并且，它显式地提供了大端序和小端序访问方法，而不是内部跟踪字节顺序。
 * 此外，它能够透明且高效地在堆内和堆外内存之间移动数据。
 *
 * <p><i>实现实现</i>: 我们使用大量本地指令支持的操作，以实现高效率。多字节类型（int、 long、 float、 double 等）
 * 使用“unsafe”本地命令读取和写入。
 *
 * <p><i>效率说明</i>: 为了避免继承不同内存类型实现时调用抽象方法带来的开销，所以不使用继承分离不同内存类型的实现。
 */
@Internal
public final class MemorySegment {

    /** 用于测试目的的系统属性，用于激活多释放段检查。 */
    public static final String CHECK_MULTIPLE_FREE_PROPERTY =
            "flink.tests.check-segment-multiple-free";

    private static final boolean checkMultipleFree =
            System.getProperties().containsKey(CHECK_MULTIPLE_FREE_PROPERTY);

    /** 创建 Unsafe 句柄。 */
    @SuppressWarnings("restriction")
    private static final sun.misc.Unsafe UNSAFE = MemoryUtils.UNSAFE;

    /** 字节数组内容的开头，相对于字节数组对象。 */
    @SuppressWarnings("restriction")
    private static final long BYTE_ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);

    /**
     * 标记字节顺序的常量。因为这是一个布尔常量，JIT 编译器可以很好地使用它来积极地消除不适用的代码路径。
     */
    private static final boolean LITTLE_ENDIAN =
            (ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN);

    // ------------------------------------------------------------------------

    /**
     * 相对于堆字节数组对象访问内存的堆字节数组对象。
     *
     * <p>如果内存是堆内内存，则是非 null，否则为 null。如果这个缓冲区存在，我们决不能 void 这个引用，否则内存段将指向堆外未定义的地址，
     * 并可能在无序执行的情况下导致段错误。
     */
    @Nullable private final byte[] heapMemory;

    /**
     * 包封装堆外内存的直接字节缓冲区。这个内存段持有对这个缓冲区的引用，只要这个内存段存在，内存就不会被释放。
     */
    @Nullable private ByteBuffer offHeapBuffer;

    /**
     * 数据的地址，相对于堆内存字节数组。如果堆内存字节数组是 null，这将成为堆外的绝对内存地址。
     */
    private long address;

    /**
     * 最后一个可寻址字节后一个字节的地址，即分段未被释放时的地址 + 大小。
     */
    private final long addressLimit;

    /** 内存段的大小（以字节为单位）。 */
    private final int size;

    /** 内存段的可选所有者。 */
    @Nullable private final Object owner;

    @Nullable private Runnable cleaner;

    /**
     * 当底层内存不安全时，不允许封装。不安全的内存可以主动释放，而无需进行引用计数。因此，从封装缓冲区访问可能会因不了解内存释放而有风险。
     */
    private final boolean allowWrap;

    private final AtomicBoolean isFreedAtomic;

    /**
     * 创建表示字节数组内存的新内存段。
     *
     * <p>由于字节数组由堆内内存支持，这个内存段将数据存储在堆内。缓冲区必须至少是 8 字节。
     *
     * <p>内存段引用所给的所有者。
     *
     * @param buffer 表示此内存段代表的内存的字节数组。
     * @param owner  被此内存段引用的所有者。
     */
    MemorySegment(@Nonnull byte[] buffer, @Nullable Object owner) {
        this.heapMemory = buffer;
        this.offHeapBuffer = null;
        this.size = buffer.length;
        this.address = BYTE_ARRAY_BASE_OFFSET;
        this.addressLimit = this.address + this.size;
        this.owner = owner;
        this.allowWrap = true;
        this.cleaner = null;
        this.isFreedAtomic = new AtomicBoolean(false);
    }

    /**
     * 创建一个表示给定直接字节缓冲区支持的内存的新内存段。注意，给定的 ByteBuffer 必须是直接的 {@link
     * java.nio.ByteBuffer#allocateDirect(int)}，否则这个方法会抛出一个 IllegalArgumentException。
     *
     * <p>内存段引用所给的所有者。
     *
     * @param buffer 表示此内存段代表的内存的字节缓冲区。
     * @param owner  被此内存段引用的所有者。
     * @throws IllegalArgumentException 如果给定的 ByteBuffer 不是直接的，则抛出此异常。
     */
    MemorySegment(@Nonnull ByteBuffer buffer, @Nullable Object owner) {
        this(buffer, owner, true, null);
    }

    /**
     * 创建一个表示给定直接字节缓冲区支持的内存的新内存段。注意，给定的 ByteBuffer 必须是直接的 {@link
     * java.nio.ByteBuffer#allocateDirect(int)}，否则这个方法会抛出一个 IllegalArgumentException。
     *
     * <p>内存段引用所给的所有者。
     *
     * @param buffer           表示此内存段代表的内存的字节缓冲区。
     * @param owner            被此内存段引用的所有者。
     * @param allowWrap        是否允许从此内存段封装 ByteBuffer。
     * @param cleaner          当内存段被释放时需要调用的清理器。
     * @throws IllegalArgumentException 如果给定的 ByteBuffer 不是直接的，则抛出此异常。
     */
    MemorySegment(
            @Nonnull ByteBuffer buffer,
            @Nullable Object owner,
            boolean allowWrap,
            @Nullable Runnable cleaner) {
        this.heapMemory = null;
        this.offHeapBuffer = buffer;
        this.size = buffer.capacity();
        this.address = getByteBufferAddress(buffer);
        this.addressLimit = this.address + this.size;
        this.owner = owner;
        this.allowWrap = allowWrap;
        this.cleaner = cleaner;
        this.isFreedAtomic = new AtomicBoolean(false);
    }

    // ------------------------------------------------------------------------
    // Memory Segment Operations
    // ------------------------------------------------------------------------

    /**
     * 获取内存段的大小（以字节为单位）。
     *
     * @return 内存段的大小。
     */
    public int size() {
        return size;
    }

    /**
     * 检查内存段是否已释放。
     *
     * @return <tt>true</tt>，如果内存段已被释放，<tt>false</tt> 否则。
     */
    @VisibleForTesting
    public boolean isFreed() {
        return address > addressLimit;
    }

    /**
     * 释放这个内存段。
     *
     * <p>调用此操作之后，无法在此内存段上进一步操作，并且会失败。只有在该内存段对象成为垃圾收集的时候，才会释放实际的内存（堆或堆外）。
     */
    public void free() {
        if (isFreedAtomic.getAndSet(true)) {
            // 内存段已经被释放
            if (checkMultipleFree) {
                throw new IllegalStateException("MemorySegment 只能释放一次！");
            }
        } else {
            // 确保我们不能放置更多数据，并触发已释放段的检查
            address = addressLimit + 1;
            offHeapBuffer = null; // 启用堆外内存的 GC
            if (cleaner != null) {
                cleaner.run();
                cleaner = null;
            }
        }
    }

    /**
     * 检查此内存段是否由堆外内存支持。
     *
     * @return <tt>true</tt>，如果内存段由堆外内存支持，<tt>false</tt>，如果由堆内内存支持。
     */
    public boolean isOffHeap() {
        return heapMemory == null;
    }

    /**
     * 返回堆内内存段的基础字节数组。
     *
     * @return 基础字节数组
     * @throws IllegalStateException 如果内存段不代表堆内内存
     */
    public byte[] getArray() {
        if (heapMemory != null) {
            return heapMemory;
        } else {
            throw new IllegalStateException("内存段不代表堆内内存");
        }
    }

    /**
     * 返回堆外内存段的基础字节缓冲区。
     *
     * @return 基础堆外缓冲区
     * @throws IllegalStateException 如果内存段不代表堆外缓冲区
     */
    public ByteBuffer getOffHeapBuffer() {
        if (offHeapBuffer != null) {
            return offHeapBuffer;
        } else {
            throw new IllegalStateException("内存段不代表堆外缓冲区");
        }
    }

    /**
     * 返回堆外内存段的内存地址。
     *
     * @return 堆外的绝对内存地址
     * @throws IllegalStateException 如果内存段不代表堆外内存
     */
    public long getAddress() {
        if (heapMemory == null) {
            return address;
        } else {
            throw new IllegalStateException("内存段不代表堆外内存");
        }
    }

    /**
     * 在 NIO ByteBuffer 中封装基础内存位于 <tt>offset</tt> 和 <tt>offset + length</tt> 之间的块。
     * ByteBuffer 的容量为完整段，偏移量和长度参数设置缓冲区的位置和限制。
     *
     * @param offset 偏移量
     * @param length 要封装为缓冲区的字节数
     * @return 以指定部分内存段为后盾的 <tt>ByteBuffer</tt>
     * @throws IndexOutOfBoundsException 如果偏移量为负数或大于内存段大小，或者偏移量加长度大于段大小
     */
    public ByteBuffer wrap(int offset, int length) {
        if (!allowWrap) {
            throw new UnsupportedOperationException(
                    "此段不支持封装。这通常表示基础内存是不安全的，因此不允许传输所有权。");
        }
        return wrapInternal(offset, length);
    }

    private ByteBuffer wrapInternal(int offset, int length) {
        if (address <= addressLimit) {
            if (heapMemory != null) {
                return ByteBuffer.wrap(heapMemory, offset, length);
            } else {
                try {
                    ByteBuffer wrapper = Preconditions.checkNotNull(offHeapBuffer).duplicate();
                    wrapper.limit(offset + length);
                    wrapper.position(offset);
                    return wrapper;
                } catch (IllegalArgumentException e) {
                    throw new IndexOutOfBoundsException();
                }
            }
        } else {
            throw new IllegalStateException("segment 已被释放");
        }
    }

    /**
     * 获取此内存段的所有者。如果未设置所有者，则返回 null。
     *
     * @return 内存段的所有者，或者如果没有所有者则为 null。
     */
    @Nullable
    public Object getOwner() {
        return owner;
    }

    // ------------------------------------------------------------------------
    // Random Access get() and put() methods
    // ------------------------------------------------------------------------
    //
    // Notes on the implementation: We try to collapse as many checks as possible.
    // We need to obey the following rules to make this safe against segfaults:
    //
    //  - 在检查和使用之前，将可变字段抓取到堆栈上。这可防止并发修改使指针失效
    //  - 使用减法进行范围检查，因为它们是宽容的
    // ------------------------------------------------------------------------

    /**
     * 读取给定位置的字节。
     *
     * @param index 要从中读取字节的位置
     * @return 给定位置的字节
     * @throws IndexOutOfBoundsException 如果索引为负数，或大于或等于内存段的大小，则抛出此异常
     */
    public byte get(int index) {
        final long pos = address + index;
        if (index >= 0 && pos < addressLimit) {
            return UNSAFE.getByte(heapMemory, pos);
        } else if (address > addressLimit) {
            throw new IllegalStateException("segment 已被释放");
        } else {
            throw new IndexOutOfBoundsException();
        }
    }

    /**
     * 在给定位置写入字节。
     *
     * @param index 要写入字节的位置
     * @param b     要写入的字节
     * @throws IndexOutOfBoundsException 如果索引为负数，或大于或等于内存段的大小，则抛出此异常
     */
    public void put(int index, byte b) {
        final long pos = address + index;
        if (index >= 0 && pos < addressLimit) {
            UNSAFE.putByte(heapMemory, pos, b);
        } else if (address > addressLimit) {
            throw new IllegalStateException("segment 已被释放");
        } else {
            throw new IndexOutOfBoundsException();
        }
    }

    /**
     * 批量读取方法。将指定位置的 dst.length 内存复制到目标内存。
     *
     * @param index 要从中读取第一个字节的位置
     * @param dst   要复制到的目标内存
     * @throws IndexOutOfBoundsException 如果索引为负数，或者太大，导致无法用目标数组填充数据
     */
    public void get(int index, byte[] dst) {
        get(index, dst, 0, dst.length);
    }

    /**
     * 批量写入方法。将源内存中的 src.length 内存从指定位置写入内存段。
     *
     * @param index 记忆体段数组中的索引，写入数据的位置
     * @param src   要从中复制数据的源数组
     * @throws IndexOutOfBoundsException 如果索引为负数，或者太大，导致数组大小超过索引和内存段结束之间的内存量
     */
    public void put(int index, byte[] src) {
        put(index, src, 0, src.length);
    }

    /**
     * 批量读取方法。从指定位置读取 length 内存，并将其复制到目标内存，从给定的偏移量开始。
     *
     * @param index      要从中读取第一个字节的位置
     * @param dst        要复制到的目标内存
     * @param offset     目标内存中的复制偏移量
     * @param length     要复制的字节数
     * @throws IndexOutOfBoundsException 如果索引为负数，或者太大，导致请求的字节数超过索引和内存段末尾之间的内存量
     */
    public void get(int index, byte[] dst, int offset, int length) {
        // 检查字节数组的偏移量和长度，以及状态
        if ((offset | length | (offset + length) | (dst.length - (offset + length))) < 0) {
            throw new IndexOutOfBoundsException();
        }

        final long pos = address + index;
        if (index >= 0 && pos <= addressLimit - length) {
            final long arrayAddress = BYTE_ARRAY_BASE_OFFSET + offset;
            UNSAFE.copyMemory(heapMemory, pos, dst, arrayAddress, length);
        } else if (address > addressLimit) {
            throw new IllegalStateException("segment 已被释放");
        } else {
            throw new IndexOutOfBoundsException(
                    String.format(
                            "pos: %d, length: %d, index: %d, offset: %d",
                            pos, length, index, offset));
        }
    }

    /**
     * 批量写入方法。从源内存中的指定位置开始，将 length 内存复制到内存段的指定位置。
     *
     * @param index 记忆体段数组中的索引，写入数据的位置
     * @param src   要从中复制数据的源数组
     * @param offset 源数组中复制开始的偏移量
     * @param length 要复制的字节数
     * @throws IndexOutOfBoundsException 如果索引为负数，或者太大，导致要复制的数组部分超过索引和内存段末尾之间的内存量
     */
    public void put(int index, byte[] src, int offset, int length) {
        // 检查字节数组的偏移量和长度
        if ((offset | length | (offset + length) | (src.length - (offset + length))) < 0) {
            throw new IndexOutOfBoundsException();
        }

        final long pos = address + index;

        if (index >= 0 && pos <= addressLimit - length) {
            final long arrayAddress = BYTE_ARRAY_BASE_OFFSET + offset;
            UNSAFE.copyMemory(src, arrayAddress, heapMemory, pos, length);
        } else if (address > addressLimit) {
            throw new IllegalStateException("segment 已被释放");
        } else {
            throw new IndexOutOfBoundsException();
        }
    }

    /**
     * 从指定位置读取一个字节，并返回其布尔表示。
     *
     * @param index 要从中读取内存的位置
     * @return 给定位置的布尔值
     * @throws IndexOutOfBoundsException 如果索引为负数，或大于段大小减一
     */
    public boolean getBoolean(int index) {
        return get(index) != 0;
    }

    /**
     * 将字节的布尔值写入此缓冲区的指定位置。
     *
     * @param index 要写入内存的位置
     * @param value 要写入的 char 值
     * @throws IndexOutOfBoundsException 如果索引为负数，或大于段大小减一
     */
    public void putBoolean(int index, boolean value) {
        put(index, (byte) (value ? 1 : 0));
    }

    /**
     * 从指定位置读取一个字符值（系统原生字节序）。
     *
     * @param index 要从中读取内存的位置
     * @return 给定位置的 char 值
     * @throws IndexOutOfBoundsException 如果索引为负数，或大于段大小减二
     */
    @SuppressWarnings("restriction")
    public char getChar(int index) {
        final long pos = address + index;
        if (index >= 0 && pos <= addressLimit - 2) {
            return UNSAFE.getChar(heapMemory, pos);
        } else if (address > addressLimit) {
            throw new IllegalStateException("segment 已被释放");
        } else {
            throw new IndexOutOfBoundsException();
        }
    }

    /**
     * 从指定位置读取一个字符值（16 位，2 字节），小端字节序。此方法的速度取决于系统的原生字节序，
     * 并可能比 {@link #getChar(int)} 慢。对于大多数情况（例如内存中的临时存储或用于 I/O 和网络的序列化），
     * 只要知道值的写入字节序和读取字节序相同就足够了，因此应该选择 {@link #getChar(int)}。
     *
     * @param index 要从中读取值的位置
     * @return 给定位置的字符值
     * @throws IndexOutOfBoundsException 如果索引为负数，或大于段大小减二
     */
    public char getCharLittleEndian(int index) {
        if (LITTLE_ENDIAN) {
            return getChar(index);
        } else {
            return Character.reverseBytes(getChar(index));
        }
    }

    /**
     * 从指定位置读取一个字符值（16 位，2 字节），大端字节序。此方法的速度取决于系统的原生字节序，
     * 并可能比 {@link #getChar(int)} 慢。对于大多数情况（例如内存中的临时存储或用于 I/O 和网络的序列化），
     * 只要知道值的写入字节序和读取字节序相同就足够了，因此应该选择 {@link #getChar(int)}。
     *
     * @param index 要从中读取值的位置
     * @return 给定位置的字符值
     * @throws IndexOutOfBoundsException 如果索引为负数，或大于段大小减二
     */
    public char getCharBigEndian(int index) {
        if (LITTLE_ENDIAN) {
            return Character.reverseBytes(getChar(index));
        } else {
            return getChar(index);
        }
    }

    /**
     * 将 char 值写入指定位置（系统原生字节序）。
     *
     * @param index 要写入内存的位置
     * @param value 要写入的 char 值
     * @throws IndexOutOfBoundsException 如果索引为负数，或大于段大小减二
     */
    @SuppressWarnings("restriction")
    public void putChar(int index, char value) {
        final long pos = address + index;
        if (index >= 0 && pos <= addressLimit - 2) {
            UNSAFE.putChar(heapMemory, pos, value);
        } else if (address > addressLimit) {
            throw new IllegalStateException("segment 已被释放");
        } else {
            throw new IndexOutOfBoundsException();
        }
    }

    /**
     * 以小端字节序将给定的字符（16 位，2 字节）写入指定位置。此方法的速度取决于系统的原生字节序，
     * 并可能比 {@link #putChar(int, char)} 慢。对于大多数情况（例如内存中的临时存储或用于 I/O 和网络的序列化），
     * 只要知道值的写入字节序和读取字节序相同就足够了，因此应该选择 {@link #putChar(int, char)}。
     *
     * @param index 要写入值的位置
     * @param value 要写入的字符
     * @throws IndexOutOfBoundsException 如果索引为负数，或大于段大小减二
     */
    public void putCharLittleEndian(int index, char value) {
        if (LITTLE_ENDIAN) {
            putChar(index, value);
        } else {
            putChar(index, Character.reverseBytes(value));
        }
    }

    /**
     * 以大端字节序将给定的字符（16 位，2 字节）写入指定位置。此方法的速度取决于系统的原生字节序，
     * 并可能比 {@link #putChar(int, char)} 慢。对于大多数情况（例如内存中的临时存储或用于 I/O 和网络的序列化），
     * 只要知道值的写入字节序和读取字节序相同就足够了，因此应该选择 {@link #putChar(int, char)}。
     *
     * @param index 要写入值的位置
     * @param value 要写入的字符
     * @throws IndexOutOfBoundsException 如果索引为负数，或大于段大小减二
     */
    public void putCharBigEndian(int index, char value) {
        if (LITTLE_ENDIAN) {
            putChar(index, Character.reverseBytes(value));
        } else {
            putChar(index, value);
        }
    }

    /**
     * 以当前字节序从指定位置读取一个 short 整数值（16 位，2 字节）。
     *
     * @param index 要从中读取内存的位置
     * @return 给定位置的 short 值
     * @throws IndexOutOfBoundsException 如果索引为负数，或大于段大小减二
     */
    public short getShort(int index) {
        final long pos = address + index;
        if (index >= 0 && pos <= addressLimit - 2) {
            return UNSAFE.getShort(heapMemory, pos);
        } else if (address > addressLimit) {
            throw new IllegalStateException("segment 已被释放");
        } else {
            throw new IndexOutOfBoundsException();
        }
    }

    /**
     * 以小端字节序从指定位置读取一个 short 整数值（16 位，2 字节）。此方法的速度取决于系统的原生字节序，
     * 并可能比 {@link #getShort(int)} 慢。对于大多数情况（例如内存中的临时存储或用于 I/O 和网络的序列化），
     * 只要知道值的写入字节序和读取字节序相同就足够了，因此应该选择 {@link #getShort(int)}。
     *
     * @param index 要从中读取值的位置
     * @return 给定位置的 short 值
     * @throws IndexOutOfBoundsException 如果索引为负数，或大于段大小减二
     */
    public short getShortLittleEndian(int index) {
        if (LITTLE_ENDIAN) {
            return getShort(index);
        } else {
            return Short.reverseBytes(getShort(index));
        }
    }

    /**
     * 以大端字节序从指定位置读取一个 short 整数值（16 位，2 字节）。此方法的速度取决于系统的原生字节序，
     * 并可能比 {@link #getShort(int)} 慢。对于大多数情况（例如内存中的临时存储或用于 I/O 和网络的序列化），
     * 只要知道值的写入字节序和读取字节序相同就足够了，因此应该选择 {@link #getShort(int)}。
     *
     * @param index 要从中读取值的位置
     * @return 给定位置的 short 值
     * @throws IndexOutOfBoundsException 如果索引为负数，或大于段大小减二
     */
    public short getShortBigEndian(int index) {
        if (LITTLE_ENDIAN) {
            return Short.reverseBytes(getShort(index));
        } else {
            return getShort(index);
        }
    }

    /**
     * 以系统原生字节序将 short 值写入指定位置。
     *
     * @param index 要写入值的位置
     * @param value 要写入的 short 值
     * @throws IndexOutOfBoundsException 如果索引为负数，或大于段大小减二
     */
    public void putShort(int index, short value) {
        final long pos = address + index;
        if (index >= 0 && pos <= addressLimit - 2) {
            UNSAFE.putShort(heapMemory, pos, value);
        } else if (address > addressLimit) {
            throw new IllegalStateException("segment 已被释放");
        } else {
            throw new IndexOutOfBoundsException();
        }
    }

    /**
     * 以小端字节序将给定的 short 整数值（16 位，2 字节）写入指定位置。此方法的速度取决于系统的原生字节序，
     * 并可能比 {@link #putShort(int, short)} 慢。对于大多数情况（例如内存中的临时存储或用于 I/O 和网络的序列化），
     * 只要知道值的写入字节序和读取字节序相同就足够了，因此应该选择 {@link #putShort(int, short)}。
     *
     * @param index 要写入值的位置
     * @param value 要写入的 short 值
     * @throws IndexOutOfBoundsException 如果索引为负数，或大于段大小减二
     */
    public void putShortLittleEndian(int index, short value) {
        if (LITTLE_ENDIAN) {
            putShort(index, value);
        } else {
            putShort(index, Short.reverseBytes(value));
        }
    }

    /**
     * 以大端字节序将给定的 short 整数值（16 位，2 字节）写入指定位置。此方法的速度取决于系统的原生字节序，
     * 并可能比 {@link #putShort(int, short)} 慢。对于大多数情况（例如内存中的临时存储或用于 I/O 和网络的序列化），
     * 只要知道值的写入字节序和读取字节序相同就足够了，因此应该选择 {@link #putShort(int, short)}。
     *
     * @param index 要写入值的位置
     * @param value 要写入的 short 值
     * @throws IndexOutOfBoundsException 如果索引为负数，或大于段大小减二
     */
    public void putShortBigEndian(int index, short value) {
        if (LITTLE_ENDIAN) {
            putShort(index, Short.reverseBytes(value));
        } else {
            putShort(index, value);
        }
    }

    /**
     * 以系统原生字节序从指定位置读取一个 int 值（32 位，4 字节）。此方法提供了最佳的整数读取速度，应尽可能使用，
     * 除非需要特定的字节序。在大多数情况下，只要知道值的写入字节序和读取字节序相同就足够了（例如内存中的临时存储或用于 I/O 和网络的序列化），
     * 因此应优先选择此方法。
     *
     * @param index 要从中读取值的位置
     * @return 给定位置的 int 值
     * @throws IndexOutOfBoundsException 如果索引为负数，或大于段大小减四
     */
    public int getInt(int index) {
        final long pos = address + index;
        if (index >= 0 && pos <= addressLimit - 4) {
            return UNSAFE.getInt(heapMemory, pos);
        } else if (address > addressLimit) {
            throw new IllegalStateException("segment 已被释放");
        } else {
            throw new IndexOutOfBoundsException();
        }
    }

    /**
     * 以小端字节序从指定位置读取一个 int 值（32 位，4 字节）。此方法的速度取决于系统的原生字节序，
     * 并可能比 {@link #getInt(int)} 慢。对于大多数情况（例如内存中的临时存储或用于 I/O 和网络的序列化），
     * 只要知道值的写入字节序和读取字节序相同就足够了，因此应该选择 {@link #getInt(int)}。
     *
     * @param index 要从中读取值的位置
     * @return 给定位置的 int 值
     * @throws IndexOutOfBoundsException 如果索引为负数，或大于段大小减四
     */
    public int getIntLittleEndian(int index) {
        if (LITTLE_ENDIAN) {
            return getInt(index);
        } else {
            return Integer.reverseBytes(getInt(index));
        }
    }

    /**
     * 以大端字节序从指定位置读取一个 int 值（32 位，4 字节）。此方法的速度取决于系统的原生字节序，
     * 并可能比 {@link #getInt(int)} 慢。对于大多数情况（例如内存中的临时存储或用于 I/O 和网络的序列化），
     * 只要知道值的写入字节序和读取字节序相同就足够了，因此应该选择 {@link #getInt(int)}。
     *
     * @param index 要从中读取值的位置
     * @return 给定位置的 int 值
     * @throws IndexOutOfBoundsException 如果索引为负数，或大于段大小减四
     */
    public int getIntBigEndian(int index) {
        if (LITTLE_ENDIAN) {
            return Integer.reverseBytes(getInt(index));
        } else {
            return getInt(index);
        }
    }

    /**
     * 以系统原生字节序将给定的 int 值（32 位，4 字节）写入指定位置。此方法提供了最佳的整数写入速度，应尽可能使用，
     * 除非需要特定的字节序。在大多数情况下，只要知道值的写入字节序和读取字节序相同就足够了（例如内存中的临时存储或用于 I/O 和网络的序列化），
     * 因此应优先选择此方法。
     *
     * @param index 要写入值的位置
     * @param value 要写入的 int 值
     * @throws IndexOutOfBoundsException 如果索引为负数，或大于段大小减四
     */
    public void putInt(int index, int value) {
        final long pos = address + index;
        if (index >= 0 && pos <= addressLimit - 4) {
            UNSAFE.putInt(heapMemory, pos, value);
        } else if (address > addressLimit) {
            throw new IllegalStateException("segment 已被释放");
        } else {
            throw new IndexOutOfBoundsException();
        }
    }

    /**
     * 以小端字节序将给定的 int 值（32 位，4 字节）写入指定位置。此方法的速度取决于系统的原生字节序，
     * 并可能比 {@link #putInt(int, int)} 慢。对于大多数情况（例如内存中的临时存储或用于 I/O 和网络的序列化），
     * 只要知道值的写入字节序和读取字节序相同就足够了，因此应该选择 {@link #putInt(int, int)}。
     *
     * @param index 要写入值的位置
     * @param value 要写入的 int 值
     * @throws IndexOutOfBoundsException 如果索引为负数，或大于段大小减四
     */
    public void putIntLittleEndian(int index, int value) {
        if (LITTLE_ENDIAN) {
            putInt(index, value);
        } else {
            putInt(index, Integer.reverseBytes(value));
        }
    }

    /**
     * 以大端字节序将给定的 int 值（32 位，4 字节）写入指定位置。此方法的速度取决于系统的原生字节序，
     * 并可能比 {@link #putInt(int, int)} 慢。对于大多数情况（例如内存中的临时存储或用于 I/O 和网络的序列化），
     * 只要知道值的写入字节序和读取字节序相同就足够了，因此应该选择 {@link #putInt(int, int)}。
     *
     * @param index 要写入值的位置
     * @param value 要写入的 int 值
     * @throws IndexOutOfBoundsException 如果索引为负数，或大于段大小减四
     */
    public void putIntBigEndian(int index, int value) {
        if (LITTLE_ENDIAN) {
            putInt(index, Integer.reverseBytes(value));
        } else {
            putInt(index, value);
        }
    }

    /**
     * 以系统原生字节序从指定位置读取一个 long 值（64 位，8 字节）。此方法提供了最佳的长整数读取速度，应尽可能使用，
     * 除非需要特定的字节序。在大多数情况下，只要知道值的写入字节序和读取字节序相同就足够了（例如内存中的临时存储或用于 I/O 和网络的序列化），
     * 因此应优先选择此方法。
     *
     * @param index 要从中读取值的位置
     * @return 给定位置的 long 值
     * @throws IndexOutOfBoundsException 如果索引为负数，或大于段大小减八
     */
    public long getLong(int index) {
        final long pos = address + index;
        if (index >= 0 && pos <= addressLimit - 8) {
            return UNSAFE.getLong(heapMemory, pos);
        } else if (address > addressLimit) {
            throw new IllegalStateException("segment 已被释放");
        } else {
            throw new IndexOutOfBoundsException();
        }
    }

    /**
     * 以小端字节序从指定位置读取一个 long 整数值（64 位，8 字节）。此方法的速度取决于系统的原生字节序，
     * 并可能比 {@link #getLong(int)} 慢。对于大多数情况（例如内存中的临时存储或用于 I/O 和网络的序列化），
     * 只要知道值的写入字节序和读取字节序相同就足够了，因此应该选择 {@link #getLong(int)}。
     *
     * @param index 要从中读取值的位置
     * @return 给定位置的 long 值
     * @throws IndexOutOfBoundsException 如果索引为负数，或大于段大小减八
     */
    public long getLongLittleEndian(int index) {
        if (LITTLE_ENDIAN) {
            return getLong(index);
        } else {
            return Long.reverseBytes(getLong(index));
        }
    }

    /**
     * 以大端字节序从指定位置读取一个 long 整数值（64 位，8 字节）。此方法的速度取决于系统的原生字节序，
     * 并可能比 {@link #getLong(int)} 慢。对于大多数情况（例如内存中的临时存储或用于 I/O 和网络的序列化），
     * 只要知道值的写入字节序和读取字节序相同就足够了，因此应该选择 {@link #getLong(int)}。
     *
     * @param index 要从中读取值的位置
     * @return 给定位置的 long 值
     * @throws IndexOutOfBoundsException 如果索引为负数，或大于段大小减八
     */
    public long getLongBigEndian(int index) {
        if (LITTLE_ENDIAN) {
            return Long.reverseBytes(getLong(index));
        } else {
            return getLong(index);
        }
    }

    /**
     * 以系统原生字节序将给定的 long 值（64 位，8 字节）写入指定位置。此方法提供了最佳的长整数写入速度，应尽可能使用，
     * 除非需要特定的字节序。在大多数情况下，只要知道值的写入字节序和读取字节序相同就足够了（例如内存中的临时存储或用于 I/O 和网络的序列化），
     * 因此应优先选择此方法。
     *
     * @param index 要写入值的位置
     * @param value 要写入的 long 值
     * @throws IndexOutOfBoundsException 如果索引为负数，或大于段大小减八
     */
    public void putLong(int index, long value) {
        final long pos = address + index;
        if (index >= 0 && pos <= addressLimit - 8) {
            UNSAFE.putLong(heapMemory, pos, value);
        } else if (address > addressLimit) {
            throw new IllegalStateException("segment 已被释放");
        } else {
            throw new IndexOutOfBoundsException();
        }
    }

    /**
     * 以小端字节序将给定的 long 值（64 位，8 字节）写入指定位置。此方法的速度取决于系统的原生字节序，
     * 并可能比 {@link #putLong(int, long)} 慢。对于大多数情况（例如内存中的临时存储或用于 I/O 和网络的序列化），
     * 只要知道值的写入字节序和读取字节序相同就足够了，因此应该选择 {@link #putLong(int, long)}。
     *
     * @param index 要写入值的位置
     * @param value 要写入的 long 值
     * @throws IndexOutOfBoundsException 如果索引为负数，或大于段大小减八
     */
    public void putLongLittleEndian(int index, long value) {
        if (LITTLE_ENDIAN) {
            putLong(index, value);
        } else {
            putLong(index, Long.reverseBytes(value));
        }
    }

    /**
     * 以大端字节序将给定的 long 值（64 位，8 字节）写入指定位置。此方法的速度取决于系统的原生字节序，
     * 并可能比 {@link #putLong(int, long)} 慢。对于大多数情况（例如内存中的临时存储或用于 I/O 和网络的序列化），
     * 只要知道值的写入字节序和读取字节序相同就足够了，因此应该选择 {@link #putLong(int, long)}。
     *
     * @param index 要写入值的位置
     * @param value 要写入的 long 值
     * @throws IndexOutOfBoundsException 如果索引为负数，或大于段大小减八
     */
    public void putLongBigEndian(int index, long value) {
        if (LITTLE_ENDIAN) {
            putLong(index, Long.reverseBytes(value));
        } else {
            putLong(index, value);
        }
    }

    /**
     * 以系统原生字节序从指定位置读取一个单精度浮点值（32 位，4 字节）。此方法提供了最佳的单精度浮点读取速度，
     * 应尽可能使用，除非需要特定的字节序。在大多数情况下，只要知道值的写入字节序和读取字节序相同就足够了（例如内存中的临时存储或用于 I/O 和网络的序列化），
     * 因此应优先选择此方法。
     *
     * @param index 要从中读取值的位置
     * @return 给定位置的单精度浮点值
     * @throws IndexOutOfBoundsException 如果索引为负数，或大于段大小减四
     */
    public float getFloat(int index) {
        return Float.intBitsToFloat(getInt(index));
    }

    /**
     * 以小端字节序从指定位置读取一个单精度浮点值（32 位，4 字节）。此方法的速度取决于系统的原生字节序，
     * 并可能比 {@link #getFloat(int)} 慢。对于大多数情况（例如内存中的临时存储或用于 I/O 和网络的序列化），
     * 只要知道值的写入字节序和读取字节序相同就足够了，因此应该选择 {@link #getFloat(int)}。
     *
     * @param index 要从中读取值的位置
     * @return 给定位置的单精度浮点值
     * @throws IndexOutOfBoundsException 如果索引为负数，或大于段大小减四
     */
    public float getFloatLittleEndian(int index) {
        return Float.intBitsToFloat(getIntLittleEndian(index));
    }

    /**
     * 以大端字节序从指定位置读取一个单精度浮点值（32 位，4 字节）。此方法的速度取决于系统的原生字节序，
     * 并可能比 {@link #getFloat(int)} 慢。对于大多数情况（例如内存中的临时存储或用于 I/O 和网络的序列化），
     * 只要知道值的写入字节序和读取字节序相同就足够了，因此应该选择 {@link #getFloat(int)}。
     *
     * @param index 要从中读取值的位置
     * @return 给定位置的单精度浮点值
     * @throws IndexOutOfBoundsException 如果索引为负数，或大于段大小减四
     */
    public float getFloatBigEndian(int index) {
        return Float.intBitsToFloat(getIntBigEndian(index));
    }

    /**
     * 以系统原生字节序将给定的单精度浮点值（32 位，4 字节）写入指定位置。此方法提供了最佳的单精度浮点写入速度，
     * 应尽可能使用，除非需要特定的字节序。在大多数情况下，只要知道值的写入字节序和读取字节序相同就足够了（例如内存中的临时存储或用于 I/O 和网络的序列化），
     * 因此应优先选择此方法。
     *
     * @param index 要写入值的位置
     * @param value 要写入的单精度浮点值
     * @throws IndexOutOfBoundsException 如果索引为负数，或大于段大小减四
     */
    public void putFloat(int index, float value) {
        putInt(index, Float.floatToRawIntBits(value));
    }

    /**
     * 以小端字节序将给定的单精度浮点值（32 位，4 字节）写入指定位置。此方法的速度取决于系统的原生字节序，
     * 并可能比 {@link #putFloat(int, float)} 慢。对于大多数情况（例如内存中的临时存储或用于 I/O 和网络的序列化），
     * 只要知道值的写入字节序和读取字节序相同就足够了，因此应该选择 {@link #putFloat(int, float)}。
     *
     * @param index 要写入值的位置
     * @param value 要写入的单精度浮点值
     * @throws IndexOutOfBoundsException 如果索引为负数，或大于段大小减四
     */
    public void putFloatLittleEndian(int index, float value) {
        putIntLittleEndian(index, Float.floatToRawIntBits(value));
    }

    /**
     * 以大端字节序将给定的单精度浮点值（32 位，4 字节）写入指定位置。此方法的速度取决于系统的原生字节序，
     * 并可能比 {@link #putFloat(int, float)} 慢。对于大多数情况（例如内存中的临时存储或用于 I/O 和网络的序列化），
     * 只要知道值的写入字节序和读取字节序相同就足够了，因此应该选择 {@link #putFloat(int, float)}。
     *
     * @param index 要写入值的位置
     * @param value 要写入的单精度浮点值
     * @throws IndexOutOfBoundsException 如果索引为负数，或大于段大小减四
     */
    public void putFloatBigEndian(int index, float value) {
        putIntBigEndian(index, Float.floatToRawIntBits(value));
    }

    /**
     * 以系统原生字节序从指定位置读取一个双精度浮点值（64 位，8 字节）。此方法提供了最佳的双精度浮点读取速度，
     * 应尽可能使用，除非需要特定的字节序。在大多数情况下，只要知道值的写入字节序和读取字节序相同就足够了（例如内存中的临时存储或用于 I/O 和网络的序列化），
     * 因此应优先选择此方法。
     *
     * @param index 要从中读取值的位置
     * @return 给定位置的双精度浮点值
     * @throws IndexOutOfBoundsException 如果索引为负数，或大于段大小减八
     */
    public double getDouble(int index) {
        return Double.longBitsToDouble(getLong(index));
    }

    /**
     * 以小端字节序从指定位置读取一个双精度浮点值（64 位，8 字节）。此方法的速度取决于系统的原生字节序，
     * 并可能比 {@link #getDouble(int)} 慢。对于大多数情况（例如内存中的临时存储或用于 I/O 和网络的序列化），
     * 只要知道值的写入字节序和读取字节序相同就足够了，因此应该选择 {@link #getDouble(int)}。
     *
     * @param index 要从中读取值的位置
     * @return 给定位置的双精度浮点值
     * @throws IndexOutOfBoundsException 如果索引为负数，或大于段大小减八
     */
    public double getDoubleLittleEndian(int index) {
        return Double.longBitsToDouble(getLongLittleEndian(index));
    }

    /**
     * 以大端字节序从指定位置读取一个双精度浮点值（64 位，8 字节）。此方法的速度取决于系统的原生字节序，
     * 并可能比 {@link #getDouble(int)} 慢。对于大多数情况（例如内存中的临时存储或用于 I/O 和网络的序列化），
     * 只要知道值的写入字节序和读取字节序相同就足够了，因此应该选择 {@link #getDouble(int)}。
     *
     * @param index 要从中读取值的位置
     * @return 给定位置的双精度浮点值
     * @throws IndexOutOfBoundsException 如果索引为负数，或大于段大小减八
     */
    public double getDoubleBigEndian(int index) {
        return Double.longBitsToDouble(getLongBigEndian(index));
    }

    /**
     * 以系统原生字节序将给定的双精度浮点值（64 位，8 字节）写入指定位置。此方法提供了最佳的双精度浮点写入速度，
     * 应尽可能使用，除非需要特定的字节序。在大多数情况下，只要知道值的写入字节序和读取字节序相同就足够了（例如内存中的临时存储或用于 I/O 和网络的序列化），
     * 因此应优先选择此方法。
     *
     * @param index 要写入值的位置
     * @param value 要写入的双精度浮点值
     * @throws IndexOutOfBoundsException 如果索引为负数，或大于段大小减八
     */
    public void putDouble(int index, double value) {
        putLong(index, Double.doubleToRawLongBits(value));
    }

    /**
     * 以小端字节序将给定的双精度浮点值（64 位，8 字节）写入指定位置。此方法的速度取决于系统的原生字节序，
     * 并可能比 {@link #putDouble(int, double)} 慢。对于大多数情况（例如内存中的临时存储或用于 I/O 和网络的序列化），
     * 只要知道值的写入字节序和读取字节序相同就足够了，因此应该选择 {@link #putDouble(int, double)}。
     *
     * @param index 要写入值的位置
     * @param value 要写入的双精度浮点值
     * @throws IndexOutOfBoundsException 如果索引为负数，或大于段大小减八
     */
    public void putDoubleLittleEndian(int index, double value) {
        putLongLittleEndian(index, Double.doubleToRawLongBits(value));
    }

    /**
     * 以大端字节序将给定的双精度浮点值（64 位，8 字节）写入指定位置。此方法的速度取决于系统的原生字节序，
     * 并可能比 {@link #putDouble(int, double)} 慢。对于大多数情况（例如内存中的临时存储或用于 I/O 和网络的序列化），
     * 只要知道值的写入字节序和读取字节序相同就足够了，因此应该选择 {@link #putDouble(int, double)}。
     *
     * @param index 要写入值的位置
     * @param value 要写入的双精度浮点值
     * @throws IndexOutOfBoundsException 如果索引为负数，或大于段大小减八
     */
    public void putDoubleBigEndian(int index, double value) {
        putLongBigEndian(index, Double.doubleToRawLongBits(value));
    }

    // -------------------------------------------------------------------------
    // Bulk Read and Write Methods
    // -------------------------------------------------------------------------

    public void get(DataOutput out, int offset, int length) throws IOException {
        if (address <= addressLimit) {
            if (heapMemory != null) {
                out.write(heapMemory, offset, length);
            } else {
                while (length >= 8) {
                    out.writeLong(getLongBigEndian(offset));
                    offset += 8;
                    length -= 8;
                }

                while (length > 0) {
                    out.writeByte(get(offset));
                    offset++;
                    length--;
                }
            }
        } else {
            throw new IllegalStateException("segment 已被释放");
        }
    }

    /**
     * 批量写入方法。将给定 DataInput 的 length 内存从位置 offset 写入内存段。
     *
     * @param in   数据输入的来源。
     * @param offset 内存段中复制块的目标位置。
     * @param length 要读取的字节数。
     * @throws IOException 如果 DataInput 在读取时遇到问题，例如文件结束符。
     */
    public void put(DataInput in, int offset, int length) throws IOException {
        if (address <= addressLimit) {
            if (heapMemory != null) {
                in.readFully(heapMemory, offset, length);
            } else {
                while (length >= 8) {
                    putLongBigEndian(offset, in.readLong());
                    offset += 8;
                    length -= 8;
                }
                while (length > 0) {
                    put(offset, in.readByte());
                    offset++;
                    length--;
                }
            }
        } else {
            throw new IllegalStateException("segment 已被释放");
        }
    }

    /**
     * 批量读取方法。从该内存段的 offset 位置开始，复制 numBytes 字节到目标 ByteBuffer。字节将被放入目标缓冲区，
     * 从缓冲区的当前位置开始。如果此方法尝试将多于目标 ByteBuffer 剩余字节数（相对于 {@link ByteBuffer#remaining()}）的字节写入，
     * 此方法将导致 java.nio.BufferOverflowException。
     *
     * @param offset     在内存段中开始读取字节的位置。
     * @param target     要复制字节到的 ByteBuffer。
     * @param numBytes   要复制的字节数。
     * @throws IndexOutOfBoundsException 如果偏移量无效，或者该段不包含从偏移量开始的指定字节数，或者目标 ByteBuffer 没有足够的空间容纳字节。
     * @throws ReadOnlyBufferException   如果目标缓冲区是只读的。
     */
    public void get(int offset, ByteBuffer target, int numBytes) {
        // 检查字节数组的偏移量和长度
        if ((offset | numBytes | (offset + numBytes)) < 0) {
            throw new IndexOutOfBoundsException();
        }
        if (target.isReadOnly()) {
            throw new ReadOnlyBufferException();
        }

        final int targetOffset = target.position();
        final int remaining = target.remaining();

        if (remaining < numBytes) {
            throw new BufferOverflowException();
        }

        if (target.isDirect()) {
            // 直接复制到目标内存
            final long targetPointer = getByteBufferAddress(target) + targetOffset;
            final long sourcePointer = address + offset;

            if (sourcePointer <= addressLimit - numBytes) {
                UNSAFE.copyMemory(heapMemory, sourcePointer, null, targetPointer, numBytes);
                target.position(targetOffset + numBytes);
            } else if (address > addressLimit) {
                throw new IllegalStateException("segment 已被释放");
            } else {
                throw new IndexOutOfBoundsException();
            }
        } else if (target.hasArray()) {
            // 直接移动到字节数组中
            get(offset, target.array(), targetOffset + target.arrayOffset(), numBytes);

            // 必须在 get() 调用之后，以确保在调用失败时字节缓冲区不会被修改
            target.position(targetOffset + numBytes);
        } else {
            // 其他类型的 ByteBuffer
            throw new IllegalArgumentException(
                    "目标缓冲区不是直接的，也没有数组。");
        }
    }

    /**
     * 批量写入方法。从给定的 ByteBuffer 中复制 numBytes 字节到此内存段。字节将从目标缓冲区的当前位置开始读取，
     * 并写入到该内存段的 offset 位置。如果此方法尝试读取多于目标 ByteBuffer 剩余字节数（相对于 {@link ByteBuffer#remaining()}）的字节，
     * 此方法将导致 java.nio.BufferUnderflowException。
     *
     * @param offset     在该内存段中开始写入字节的位置。
     * @param source     要从中复制字节的 ByteBuffer。
     * @param numBytes   要复制的字节数。
     * @throws IndexOutOfBoundsException 如果偏移量无效，或者源缓冲区不包含指定的字节数，或者该段没有足够的空间来容纳从 offset 开始的字节。
     */
    public void put(int offset, ByteBuffer source, int numBytes) {
        // 检查字节数组的偏移量和长度
        if ((offset | numBytes | (offset + numBytes)) < 0) {
            throw new IndexOutOfBoundsException();
        }

        final int sourceOffset = source.position();
        final int remaining = source.remaining();

        if (remaining < numBytes) {
            throw new BufferUnderflowException();
        }

        if (source.isDirect()) {
            // 直接复制到目标内存
            final long sourcePointer = getByteBufferAddress(source) + sourceOffset;
            final long targetPointer = address + offset;

            if (targetPointer <= addressLimit - numBytes) {
                UNSAFE.copyMemory(null, sourcePointer, heapMemory, targetPointer, numBytes);
                source.position(sourceOffset + numBytes);
            } else if (address > addressLimit) {
                throw new IllegalStateException("segment 已被释放");
            } else {
                throw new IndexOutOfBoundsException();
            }
        } else if (source.hasArray()) {
            // 直接移动到字节数组中
            put(offset, source.array(), sourceOffset + source.arrayOffset(), numBytes);

            // 必须在 get() 调用之后，以确保在调用失败时字节缓冲区不会被修改
            source.position(sourceOffset + numBytes);
        } else {
            // 其他类型的 ByteBuffer
            for (int i = 0; i < numBytes; i++) {
                put(offset++, source.get());
            }
        }
    }

    /**
     * 批量复制方法。从该内存段的 offset 位置开始，复制 numBytes 字节到目标内存段。字节将被放入目标段的 targetOffset 位置。
     *
     * @param offset         在该内存段中开始读取字节的位置。
     * @param target         要复制字节到的内存段。
     * @param targetOffset   目标内存段复制块的目标位置。
     * @param numBytes       要复制的字节数。
     * @throws IndexOutOfBoundsException 如果任何一个偏移量无效，或者源段不包含从 offset 开始的指定字节数，或者目标段没有足够的空间来容纳从 targetOffset 开始的字节。
     */
    public void copyTo(int offset, MemorySegment target, int targetOffset, int numBytes) {
        final byte[] thisHeapRef = this.heapMemory;
        final byte[] otherHeapRef = target.heapMemory;
        final long thisPointer = this.address + offset;
        final long otherPointer = target.address + targetOffset;

        if ((numBytes | offset | targetOffset) >= 0
                && thisPointer <= this.addressLimit - numBytes
                && otherPointer <= target.addressLimit - numBytes) {
            UNSAFE.copyMemory(thisHeapRef, thisPointer, otherHeapRef, otherPointer, numBytes);
        } else if (this.address > this.addressLimit) {
            throw new IllegalStateException("此内存段已被释放。");
        } else if (target.address > target.addressLimit) {
            throw new IllegalStateException("目标内存段已被释放。");
        } else {
            throw new IndexOutOfBoundsException(
                    String.format(
                            "offset=%d, targetOffset=%d, numBytes=%d, address=%d, targetAddress=%d",
                            offset, targetOffset, numBytes, this.address, target.address));
        }
    }

    /**
     * 批量复制方法。将 numBytes 字节复制到目标不安全的对象和指针。注意：这是一个不安全的方法，此处没有检查，因此请小心。
     *
     * @param offset         在该内存段中开始读取字节的位置。
     * @param target         要复制字节到的不安全内存。
     * @param targetPointer  目标不安全内存复制块的目标位置。
     * @param numBytes       要复制的字节数。
     * @throws IndexOutOfBoundsException 如果源段不包含从 offset 开始的指定字节数。
     */
    public void copyToUnsafe(int offset, Object target, int targetPointer, int numBytes) {
        final long thisPointer = this.address + offset;
        if (thisPointer + numBytes > addressLimit) {
            throw new IndexOutOfBoundsException(
                    String.format(
                            "offset=%d, numBytes=%d, address=%d", offset, numBytes, this.address));
        }
        UNSAFE.copyMemory(this.heapMemory, thisPointer, target, targetPointer, numBytes);
    }

    /**
     * 批量复制方法。从源不安全对象和指针复制 numBytes 字节。注意：这是一个不安全的方法，此处没有检查，因此请小心。
     *
     * @param offset         在该内存段中开始写入字节的位置。
     * @param source         要从中复制字节的不安全内存。
     * @param sourcePointer  源不安全内存复制块的来源位置。
     * @param numBytes       要复制的字节数。
     * @throws IndexOutOfBoundsException 如果该段无法容纳从 offset 开始的指定字节数。
     */
    public void copyFromUnsafe(int offset, Object source, int sourcePointer, int numBytes) {
        final long thisPointer = this.address + offset;
        if (thisPointer + numBytes > addressLimit) {
            throw new IndexOutOfBoundsException(
                    String.format(
                            "offset=%d, numBytes=%d, address=%d", offset, numBytes, this.address));
        }
        UNSAFE.copyMemory(source, sourcePointer, this.heapMemory, thisPointer, numBytes);
    }

    // -------------------------------------------------------------------------
    // Comparisons & Swapping
    // -------------------------------------------------------------------------

    /**
     * 比较两个内存段区域。
     *
     * @param seg2   与之比较的段
     * @param offset1     该段的起始比较偏移量
     * @param offset2     seg2 的起始比较偏移量
     * @param len 需要比较的内存区域的长度
     * @return 如果相等则为 0，如果 seg1 < seg2 则为 -1，否则为 1
     */
    public int compare(MemorySegment seg2, int offset1, int offset2, int len) {
        while (len >= 8) {
            long l1 = this.getLongBigEndian(offset1);
            long l2 = seg2.getLongBigEndian(offset2);

            if (l1 != l2) {
                return (l1 < l2) ^ (l1 < 0) ^ (l2 < 0) ? -1 : 1;
            }

            offset1 += 8;
            offset2 += 8;
            len -= 8;
        }
        while (len > 0) {
            int b1 = this.get(offset1) & 0xff;
            int b2 = seg2.get(offset2) & 0xff;
            int cmp = b1 - b2;
            if (cmp != 0) {
                return cmp;
            }
            offset1++;
            offset2++;
            len--;
        }
        return 0;
    }

    /**
     * 比较具有不同长度的两个内存段区域。
     *
     * @param seg2   与之比较的段
     * @param offset1     该段的起始比较偏移量
     * @param offset2     seg2 的起始比较偏移量
     * @param len1 该内存区域的长度
     * @param len2 seg2 的长度
     * @return 如果相等则为 0，如果 seg1 < seg2 则为 -1，否则为 1
     */
    public int compare(MemorySegment seg2, int offset1, int offset2, int len1, int len2) {
        final int minLength = Math.min(len1, len2);
        int c = compare(seg2, offset1, offset2, minLength);
        return c == 0 ? (len1 - len2) : c;
    }

    /**
     * 使用给定的辅助缓冲区交换两个内存段的字节。
     *
     * @param tempBuffer   在三角交换中放置数据的辅助缓冲区。
     * @param seg2         要交换字节的段
     * @param offset1     该段的起始交换偏移量
     * @param offset2     seg2 的起始交换偏移量
     * @param len 需要交换的内存区域的长度
     */
    public void swapBytes(
            byte[] tempBuffer, MemorySegment seg2, int offset1, int offset2, int len) {
        if ((offset1 | offset2 | len | (tempBuffer.length - len)) >= 0) {
            final long thisPos = this.address + offset1;
            final long otherPos = seg2.address + offset2;

            if (thisPos <= this.addressLimit - len && otherPos <= seg2.addressLimit - len) {
                // this -> temp buffer
                UNSAFE.copyMemory(
                        this.heapMemory, thisPos, tempBuffer, BYTE_ARRAY_BASE_OFFSET, len);

                // other -> this
                UNSAFE.copyMemory(seg2.heapMemory, otherPos, this.heapMemory, thisPos, len);

                // temp buffer -> other
                UNSAFE.copyMemory(
                        tempBuffer, BYTE_ARRAY_BASE_OFFSET, seg2.heapMemory, otherPos, len);
                return;
            } else if (this.address > this.addressLimit) {
                throw new IllegalStateException("该内存段已被释放。");
            } else if (seg2.address > seg2.addressLimit) {
                throw new IllegalStateException("其他内存段已被释放。");
            }
        }

        // 偏移量无效
        throw new IndexOutOfBoundsException(
                String.format(
                        "offset1=%d, offset2=%d, len=%d, bufferSize=%d, address1=%d, address2=%d",
                        offset1, offset2, len, tempBuffer.length, this.address, seg2.address));
    }

    /**
     * 比较两个内存段区域。
     *
     * @param seg2   与此段比较的段
     * @param offset1     该段的起始比较偏移量
     * @param offset2     seg2 的起始比较偏移量
     * @param length 需要比较的内存区域的长度
     * @return 如果相等则为 true，否则为 false
     */
    public boolean equalTo(MemorySegment seg2, int offset1, int offset2, int length) {
        int i = 0;

        // 我们假设支持未对齐的访问。
        // 每次比较 8 个字节。
        while (i <= length - 8) {
            if (getLong(offset1 + i) != seg2.getLong(offset2 + i)) {
                return false;
            }
            i += 8;
        }

        // 处理剩下的 (length % 8) 字节。
        while (i < length) {
            if (get(offset1 + i) != seg2.get(offset2 + i)) {
                return false;
            }
            i += 1;
        }

        return true;
    }

    /**
     * 获取堆字节数组对象。
     *
     * @return 如果内存位于堆内，则返回非 null，否则返回 null。
     */
    public byte[] getHeapMemory() {
        return heapMemory;
    }

    /**
     * 将表示整个段的 ByteBuffer 应用于给定的处理函数。
     *
     * <p>注意：传递给处理函数的 ByteBuffer 是临时的，并且在处理之后可能会变得无效。因此，处理函数不应该尝试保留 ByteBuffer 的任何引用。
     *
     * @param processFunction 应用到段的 ByteBuffer 的处理函数。
     * @return 处理函数返回的值。
     */
    public <T> T processAsByteBuffer(Function<ByteBuffer, T> processFunction) {
        return Preconditions.checkNotNull(processFunction).apply(wrapInternal(0, size));
    }

    /**
     * 将表示整个段的 ByteBuffer 提供给给定的处理消费者。
     *
     * <p>注意：传递给处理消费者的 ByteBuffer 是临时的，并且在处理之后可能会变得无效。因此，处理消费者不应该尝试保留 ByteBuffer 的任何引用。
     *
     * @param processConsumer 要接受段的 ByteBuffer 的处理消费者。
     */
    public void processAsByteBuffer(Consumer<ByteBuffer> processConsumer) {
        Preconditions.checkNotNull(processConsumer).accept(wrapInternal(0, size));
    }
}
