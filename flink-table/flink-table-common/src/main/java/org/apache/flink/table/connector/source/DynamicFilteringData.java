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

package org.apache.flink.table.connector.source;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArrayComparator;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.io.ByteArrayInputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** 数据用于动态过滤。 */
@PublicEvolving
public class DynamicFilteringData implements Serializable {

    // 行数据的类型信息，用于序列化和反序列化行数据
    private final TypeInformation<RowData> typeInfo;

    // 行的结构信息，包括字段的数量和类型
    private final RowType rowType;

    /**
     * 用于过滤的序列化行数据列表。行值的类型必须是 Flink 内部数据类型，即由 FieldGetter 返回的类型。
     * 该列表应按排序且去重。
     */
    private final List<byte[]> serializedData;

    // 是否实际应用过滤逻辑。如果为 false，则所有数据都被认为是包含的
    private final boolean isFiltering;

    // 动态过滤数据是否已准备就绪的标志
    private transient volatile boolean prepared = false;

    // 存储过滤数据的哈希映射，键为哈希值，值为对应的行数据列表
    private transient Map<Integer, List<RowData>> dataMap;

    // 字段获取器数组，用于从 RowData 中提取字段值
    private transient RowData.FieldGetter[] fieldGetters;

    // 构造函数，初始化动态过滤数据
    public DynamicFilteringData(
            TypeInformation<RowData> typeInfo,
            RowType rowType,
            List<byte[]> serializedData,
            boolean isFiltering) {
        this.typeInfo = checkNotNull(typeInfo); // 检查 typeInfo 不为空
        this.rowType = checkNotNull(rowType); // 检查 rowType 不为空
        this.serializedData = checkNotNull(serializedData); // 检查 serializedData 不为空
        this.isFiltering = isFiltering; // 设置是否过滤
    }

    // 返回是否应用过滤逻辑，true 表示过滤，false 表示不过滤
    public boolean isFiltering() {
        return isFiltering;
    }

    // 返回行的结构信息
    public RowType getRowType() {
        return rowType;
    }

    /**
     * 判断动态过滤数据是否包含指定的行数据。
     *
     * @param row 要测试的行数据。行值的类型必须是 Flink 内部数据类型，即由 FieldGetter 返回的类型。
     * @return 如果动态过滤数据包含指定的行，返回 true，否则返回 false。
     */
    public boolean contains(RowData row) {
        if (!isFiltering) { // 如果过滤未开启，直接返回 true
            return true;
        } else if (row.getArity() != rowType.getFieldCount()) { // 检查行的字段数量是否匹配
            throw new TableException("RowData 的字段数不匹配");
        } else {
            prepare(); // 准备过滤数据
            List<RowData> mayMatchRowData = dataMap.get(hash(row)); // 获取可能匹配的行数据
            if (mayMatchRowData == null) { // 如果没有匹配项，返回 false
                return false;
            }
            for (RowData mayMatch : mayMatchRowData) { // 遍历可能匹配的行数据
                if (matchRow(row, mayMatch)) { // 如果行匹配，返回 true
                    return true;
                }
            }
            return false; // 遍历完成后未找到匹配，返回 false
        }
    }

    // 比较两行数据是否匹配
    private boolean matchRow(RowData row, RowData mayMatch) {
        for (int i = 0; i < rowType.getFieldCount(); ++i) { // 遍历所有字段
            if (!Objects.equals(
                    fieldGetters[i].getFieldOrNull(row), // 获取第一个行数据的字段值
                    fieldGetters[i].getFieldOrNull(mayMatch))) { // 获取第二个行数据的字段值
                return false; // 如果字段不同，返回 false
            }
        }
        return true; // 所有字段都相同，返回 true
    }

    // 准备过滤数据的逻辑
    private void prepare() {
        if (!prepared) { // 如果未准备就绪
            synchronized (this) { // 同步块，确保线程安全
                if (!prepared) { // 再次检查是否已准备，避免重复准备
                    doPrepare(); // 调用实际的准备逻辑
                    prepared = true; // 设置为已准备
                }
            }
        }
    }

    // 执行过滤数据的准备逻辑
    private void doPrepare() {
        this.dataMap = new HashMap<>(); // 初始化数据哈希映射
        if (isFiltering) { // 如果过滤开启
            // 创建字段获取器数组
            this.fieldGetters = IntStream.range(0, rowType.getFieldCount())
                    .mapToObj(i -> RowData.createFieldGetter(rowType.getTypeAt(i), i))
                    .toArray(RowData.FieldGetter[]::new);

            // 创建序列化器，用于反序列化数据
            TypeSerializer<RowData> serializer =
                    typeInfo.createSerializer(new SerializerConfigImpl());

            // 遍历序列化数据并反序列化
            for (byte[] bytes : serializedData) {
                try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
                     DataInputViewStreamWrapper inView = new DataInputViewStreamWrapper(bais)) {
                    RowData partition = serializer.deserialize(inView); // 反序列化成 RowData
                    // 使用哈希值作为键，将行数据存入哈希映射
                    dataMap.computeIfAbsent(hash(partition), k -> new ArrayList<>())
                            .add(partition);
                } catch (Exception e) {
                    throw new TableException("无法反序列化数据", e);
                }
            }
        }
    }

    // 计算行数据的哈希值
    private int hash(RowData row) {
        // 从字段获取器获取字段值，并计算哈希值
        return Objects.hash(Arrays.stream(fieldGetters)
                .map(g -> g.getFieldOrNull(row))
                .toArray());
    }

    // 比较两个 DynamicFilteringData 对象是否相等
    public static boolean isEqual(DynamicFilteringData data, DynamicFilteringData another) {
        if (data == null) { // 如果 data 为 null，检查 another 是否也为 null
            return another == null;
        }
        // 检查另一个对象是否为 null，过滤是否开启、类型信息、行类型和序列化数据大小是否相同
        if (another == null
                || (data.isFiltering != another.isFiltering)
                || !data.typeInfo.equals(another.typeInfo)
                || !data.rowType.equals(another.rowType)
                || data.serializedData.size() != another.serializedData.size()) {
            return false;
        }

        // 比较序列化数据的内容是否相同
        BytePrimitiveArrayComparator comparator = new BytePrimitiveArrayComparator(true);
        for (int i = 0; i < data.serializedData.size(); i++) {
            if (comparator.compare(data.serializedData.get(i), another.serializedData.get(i)) != 0) {
                return false;
            }
        }
        return true;
    }

    // 用于测试的 API，返回过滤数据
    @VisibleForTesting
    public Collection<RowData> getData() {
        prepare(); // 确保数据已准备
        // 将哈希映射中的值合并为一个列表并返回
        return dataMap.values().stream().flatMap(List::stream).collect(Collectors.toList());
    }

    // 返回动态过滤数据的字符串表示
    @Override
    public String toString() {
        return "DynamicFilteringData{"
                + "isFiltering="
                + isFiltering // 是否过滤
                + ", data size="
                + serializedData.size() // 序列化数据的大小
                + '}';
    }
}
