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

package org.apache.flink.table.data;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.types.RowKind;

import javax.annotation.Nullable;

import java.io.Serializable;

import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getFieldCount;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getPrecision;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getScale;

/**
 * 表生态系统中用于表示数据的内部数据结构的基本接口，支持 {@link RowType} 和其他（可能嵌套的）结构化类型（如 {@link StructuredType}）。
 *
 * <p>所有在 Table API 或 SQL 管道中传输的顶级记录都是此接口的实例。每个 {@link RowData} 包含一个 {@link RowKind}，表示该行在变更日志中描述的变更类型。
 * {@link RowKind} 仅是行的元数据信息，不属于表的 Schema，即不是专门的字段。
 *
 * <p>注意：此数据结构的所有字段必须是内部数据结构。
 *
 * <p>{@link RowData} 接口有多种实现，适用于不同的场景：
 *
 * <ul>
 *   <li>二进制导向的实现 {@code BinaryRowData} 使用 {@link MemorySegment} 引用而不是 Java 对象，以减少序列化/反序列化的开销。
 *   <li>面向对象的实现 {@link GenericRowData} 使用 Java {@link Object} 数组，易于构造且更新高效。
 * </ul>
 *
 * <p>{@link GenericRowData} 适用于公共使用，行为稳定。如果需要内部数据结构，建议使用此类构造 {@link RowData} 实例。
 *
 * <p>Flink 的 Table API 和 SQL 数据类型与内部数据结构的映射关系如下表所示：
 *
 * <pre>
 * +--------------------------------+-----------------------------------------+
 * | SQL 数据类型                   | 内部数据结构                            |
 * +--------------------------------+-----------------------------------------+
 * | BOOLEAN                        | boolean                                 |
 * +--------------------------------+-----------------------------------------+
 * | CHAR / VARCHAR / STRING        | {@link StringData}                      |
 * +--------------------------------+-----------------------------------------+
 * | BINARY / VARBINARY / BYTES     | byte[]                                  |
 * +--------------------------------+-----------------------------------------+
 * | DECIMAL                        | {@link DecimalData}                     |
 * +--------------------------------+-----------------------------------------+
 * | TINYINT                        | byte                                    |
 * +--------------------------------+-----------------------------------------+
 * | SMALLINT                       | short                                   |
 * +--------------------------------+-----------------------------------------+
 * | INT                            | int                                     |
 * +--------------------------------+-----------------------------------------+
 * | BIGINT                         | long                                    |
 * +--------------------------------+-----------------------------------------+
 * | FLOAT                          | float                                   |
 * +--------------------------------+-----------------------------------------+
 * | DOUBLE                         | double                                  |
 * +--------------------------------+-----------------------------------------+
 * | DATE                           | int（自纪元以来的天数）                 |
 * +--------------------------------+-----------------------------------------+
 * | TIME                           | int（一天中的毫秒数）                   |
 * +--------------------------------+-----------------------------------------+
 * | TIMESTAMP                      | {@link TimestampData}                   |
 * +--------------------------------+-----------------------------------------+
 * | TIMESTAMP WITH LOCAL TIME ZONE | {@link TimestampData}                   |
 * +--------------------------------+-----------------------------------------+
 * | INTERVAL YEAR TO MONTH         | int（月份数）                           |
 * +--------------------------------+-----------------------------------------+
 * | INTERVAL DAY TO SECOND         | long（毫秒数）                          |
 * +--------------------------------+-----------------------------------------+
 * | ROW / structured types         | {@link RowData}                         |
 * +--------------------------------+-----------------------------------------+
 * | ARRAY                          | {@link ArrayData}                       |
 * +--------------------------------+-----------------------------------------+
 * | MAP / MULTISET                 | {@link MapData}                         |
 * +--------------------------------+-----------------------------------------+
 * | RAW                            | {@link RawValueData}                    |
 * +--------------------------------+-----------------------------------------+
 * </pre>
 *
 * <p>可空性始终由容器数据结构处理。
 */
/**
 * @授课老师: 码界探索
 * @微信: 252810631
 * @版权所有: 请尊重劳动成果
 * 表生态系统中用于表示数据的内部数据结构的基本接口，支持 {@link RowType} 和其他（可能嵌套的）结构化类型（如 {@link StructuredType}）。
 */
@PublicEvolving
public interface RowData {

    /**
     * 返回此行中的字段数量。
     *
     * <p>该数量不包括 {@link RowKind}，{@link RowKind} 是单独存储的。
     */
    int getArity();

    /**
     * 返回此行在变更日志中描述的变更类型。
     *
     * @see RowKind
     */
    RowKind getRowKind();

    /**
     * 设置此行在变更日志中描述的变更类型。
     *
     * @see RowKind
     */
    void setRowKind(RowKind kind);

    // ------------------------------------------------------------------------------------------
    // 只读访问方法
    // ------------------------------------------------------------------------------------------

    /**
     * 如果指定位置的字段为 null，则返回 true。
     */
    boolean isNullAt(int pos);

    /**
     * 返回指定位置的布尔值。
     */
    boolean getBoolean(int pos);

    /**
     * 返回指定位置的字节值。
     */
    byte getByte(int pos);

    /**
     * 返回指定位置的短整型值。
     */
    short getShort(int pos);

    /**
     * 返回指定位置的整型值。
     */
    int getInt(int pos);

    /**
     * 返回指定位置的长整型值。
     */
    long getLong(int pos);

    /**
     * 返回指定位置的浮点型值。
     */
    float getFloat(int pos);

    /**
     * 返回指定位置的双精度浮点型值。
     */
    double getDouble(int pos);

    /**
     * 返回指定位置的字符串值。
     */
    StringData getString(int pos);

    /**
     * 返回指定位置的 Decimal 值。
     *
     * <p>需要精度和小数位数来确定 Decimal 值是否以紧凑格式存储（参见 {@link DecimalData}）。
     */
    DecimalData getDecimal(int pos, int precision, int scale);

    /**
     * 返回指定位置的 Timestamp 值。
     *
     * <p>需要精度来确定 Timestamp 值是否以紧凑格式存储（参见 {@link TimestampData}）。
     */
    TimestampData getTimestamp(int pos, int precision);

    /**
     * 返回指定位置的原始值。
     *
     * @param <T> 原始值的类型
     */
    <T> RawValueData<T> getRawValue(int pos);

    /**
     * 返回指定位置的二进制值。
     */
    byte[] getBinary(int pos);

    /**
     * 返回指定位置的数组值。
     */
    ArrayData getArray(int pos);

    /**
     * 返回指定位置的 Map 值。
     */
    MapData getMap(int pos);

    /**
     * 返回指定位置的行值。
     *
     * <p>需要字段数量以正确提取行。
     */
    RowData getRow(int pos, int numFields);

    // ------------------------------------------------------------------------------------------
    // 访问工具
    // ------------------------------------------------------------------------------------------

    /**
     * 为给定位置的内部行数据结构创建一个访问器，用于获取元素。
     *
     * @param fieldType 行元素的类型
     * @param fieldPos 行元素的位置
     * @return 创建的字段访问器
     */
    static FieldGetter createFieldGetter(LogicalType fieldType, int fieldPos) {
        FieldGetter fieldGetter;
        // 按类型根定义排序
        switch (fieldType.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                fieldGetter = row -> row.getString(fieldPos);
                break;
            case BOOLEAN:
                fieldGetter = row -> row.getBoolean(fieldPos);
                break;
            case BINARY:
            case VARBINARY:
                fieldGetter = row -> row.getBinary(fieldPos);
                break;
            case DECIMAL:
                final int decimalPrecision = getPrecision(fieldType);
                final int decimalScale = getScale(fieldType);
                fieldGetter = row -> row.getDecimal(fieldPos, decimalPrecision, decimalScale);
                break;
            case TINYINT:
                fieldGetter = row -> row.getByte(fieldPos);
                break;
            case SMALLINT:
                fieldGetter = row -> row.getShort(fieldPos);
                break;
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case INTERVAL_YEAR_MONTH:
                fieldGetter = row -> row.getInt(fieldPos);
                break;
            case BIGINT:
            case INTERVAL_DAY_TIME:
                fieldGetter = row -> row.getLong(fieldPos);
                break;
            case FLOAT:
                fieldGetter = row -> row.getFloat(fieldPos);
                break;
            case DOUBLE:
                fieldGetter = row -> row.getDouble(fieldPos);
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                final int timestampPrecision = getPrecision(fieldType);
                fieldGetter = row -> row.getTimestamp(fieldPos, timestampPrecision);
                break;
            case TIMESTAMP_WITH_TIME_ZONE:
                throw new UnsupportedOperationException();
            case ARRAY:
                fieldGetter = row -> row.getArray(fieldPos);
                break;
            case MULTISET:
            case MAP:
                fieldGetter = row -> row.getMap(fieldPos);
                break;
            case ROW:
            case STRUCTURED_TYPE:
                final int rowFieldCount = getFieldCount(fieldType);
                fieldGetter = row -> row.getRow(fieldPos, rowFieldCount);
                break;
            case DISTINCT_TYPE:
                fieldGetter = createFieldGetter(((DistinctType) fieldType).getSourceType(), fieldPos);
                break;
            case RAW:
                fieldGetter = row -> row.getRawValue(fieldPos);
                break;
            case NULL:
            case SYMBOL:
            case UNRESOLVED:
            default:
                throw new IllegalArgumentException();
        }
        if (!fieldType.isNullable()) {
            return fieldGetter;
        }
        return row -> {
            if (row.isNullAt(fieldPos)) {
                return null;
            }
            return fieldGetter.getFieldOrNull(row);
        };
    }

    /**
     * 用于在运行时获取行字段的访问器。
     *
     * @see #createFieldGetter(LogicalType, int)
     */
    @PublicEvolving
    interface FieldGetter extends Serializable {
        @Nullable
        Object getFieldOrNull(RowData row);
    }
}
