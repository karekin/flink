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

package org.apache.flink.table.planner.plan.nodes.exec.common;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.typeutils.InputTypeConfigurable;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.datastream.CustomSinkOperatorUidHashes;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.OutputFormatSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.transformations.LegacySinkTransformation;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.runtime.partitioner.KeyGroupStreamPartitioner;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.ParallelismProvider;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.DynamicTableSink.SinkRuntimeProvider;
import org.apache.flink.table.connector.sink.OutputFormatProvider;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.connector.sink.SinkProvider;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.connector.sink.abilities.SupportsRowLevelDelete;
import org.apache.flink.table.connector.sink.abilities.SupportsRowLevelUpdate;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.connectors.TransformationSinkProvider;
import org.apache.flink.table.planner.plan.abilities.sink.RowLevelDeleteSpec;
import org.apache.flink.table.planner.plan.abilities.sink.RowLevelUpdateSpec;
import org.apache.flink.table.planner.plan.abilities.sink.SinkAbilitySpec;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.MultipleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.spec.DynamicTableSinkSpec;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.nodes.exec.utils.TransformationMetadata;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.sink.ConstraintEnforcer;
import org.apache.flink.table.runtime.operators.sink.RowKindSetter;
import org.apache.flink.table.runtime.operators.sink.SinkOperator;
import org.apache.flink.table.runtime.operators.sink.StreamRecordTimestampInserter;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.TemporaryClassLoaderContext;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Base {@link ExecNode} to write data to an external sink defined by a {@link DynamicTableSink}.
 */
public abstract class CommonExecSink extends ExecNodeBase<Object>
        implements MultipleTransformationTranslator<Object> {

    // 静态常量定义，标识不同的 Transformation 类型
    // 这些常量用于区分在数据处理过程中生成的不同类型的 Transformation
    public static final String CONSTRAINT_VALIDATOR_TRANSFORMATION = "constraint-validator";
    public static final String PARTITIONER_TRANSFORMATION = "partitioner";
    public static final String UPSERT_MATERIALIZE_TRANSFORMATION = "upsert-materialize";
    public static final String TIMESTAMP_INSERTER_TRANSFORMATION = "timestamp-inserter";
    public static final String ROW_KIND_SETTER = "row-kind-setter";
    public static final String SINK_TRANSFORMATION = "sink";

    // 静态常量定义，标识表汇规格字段名称
    // 用于 JSON 反序列化时识别表汇规格
    public static final String FIELD_NAME_DYNAMIC_TABLE_SINK = "dynamicTableSink";

    // 使用 JSON 注解声明的表汇规格字段
    // 用于 JSON 反序列化，存储表汇的相关配置信息
    @JsonProperty(FIELD_NAME_DYNAMIC_TABLE_SINK)
    protected final DynamicTableSinkSpec tableSinkSpec;

    // 输入数据的变更日志模式
    // 表示输入数据包含哪种类型的变更（插入、更新、删除等）
    private final ChangelogMode inputChangelogMode;

    // 是否输入数据是有限的（有界）
    // 有界数据表示数据流有明确的开始和结束
    private final boolean isBounded;

    // 标记是否显式配置了数据汇的并行度
    // 用于控制数据汇操作符的并行度配置逻辑
    protected boolean sinkParallelismConfigured;

    /**
     * 构造函数，用于初始化 CommonExecSink。
     *
     * @param id                      节点的唯一标识符
     * @param context                 执行节点的上下文对象
     * @param persistedConfig         保存的配置信息
     * @param tableSinkSpec           表汇规格，定义了数据汇的行为
     * @param inputChangelogMode      输入数据的变更日志模式
     * @param isBounded               是否输入数据是有限的（有界）
     * @param inputProperties         输入的属性列表
     * @param outputType              输出的数据类型
     * @param description             节点的描述信息
     */
    protected CommonExecSink(
            int id,
            ExecNodeContext context,
            ReadableConfig persistedConfig,
            DynamicTableSinkSpec tableSinkSpec,
            ChangelogMode inputChangelogMode,
            boolean isBounded,
            List<InputProperty> inputProperties,
            LogicalType outputType,
            String description) {
        // 调用父类的构造函数
        super(id, context, persistedConfig, inputProperties, outputType, description);
        // 初始化表汇规格
        this.tableSinkSpec = tableSinkSpec;
        // 初始化输入变更日志模式
        this.inputChangelogMode = inputChangelogMode;
        // 初始化是否有限标识
        this.isBounded = isBounded;
    }

    /**
     * 获取简化的节点名称，通常基于表汇的身份标识。
     *
     * @return 表汇的身份标识
     */
    @Override
    public String getSimplifiedName() {
        // 从表汇规格中获取解析后的表的标识符
        return tableSinkSpec.getContextResolvedTable().getIdentifier().getObjectName();
    }

    /**
     * 获取表汇规格，用于生成数据汇的 Transformation。
     *
     * @return 表汇规格对象
     */
    public DynamicTableSinkSpec getTableSinkSpec() {
        return tableSinkSpec;
    }

    /**
     * 创建一个数据汇的 Transformation，用于将数据写入外部系统。
     *
     * @param streamExecEnv Flink 的流执行环境，用于配置和执行流处理作业
     * @param config        执行节点的配置信息
     * @param classLoader   类加载器，用于加载用户定义的函数或类
     * @param inputTransform 输入的 Transformation，表示数据源或之前处理步骤的输出
     * @param tableSink     动态表汇，定义了如何将数据写入外部系统
     * @param rowtimeFieldIndex 行时间字段的索引，用于时间相关的处理
     * @param upsertMaterialize 是否将更新（upsert）操作物化（转换为插入或删除操作）
     * @param inputUpsertKey 输入数据中用于更新操作的键的索引数组
     * @return 创建的 Transformation 对象
     */
    @SuppressWarnings("unchecked")
    protected Transformation<Object> createSinkTransformation(
            StreamExecutionEnvironment streamExecEnv,
            ExecNodeConfig config,
            ClassLoader classLoader,
            Transformation<RowData> inputTransform,
            DynamicTableSink tableSink,
            int rowtimeFieldIndex,
            boolean upsertMaterialize,
            int[] inputUpsertKey) {
        // 从表汇规格中获取解析后的 Schema
        final ResolvedSchema schema = tableSinkSpec.getContextResolvedTable().getResolvedSchema();
        // 获取数据汇的运行时提供者，这通常与具体的外部系统相关
        final SinkRuntimeProvider runtimeProvider = tableSink.getSinkRuntimeProvider(
                new SinkRuntimeProviderContext(isBounded, tableSinkSpec.getTargetColumns()));
        // 根据解析后的 Schema 获取物理行类型
        final RowType physicalRowType = getPhysicalRowType(schema);
        // 获取主键的索引数组，如果表定义了主键的话
        final int[] primaryKeys = getPrimaryKeyIndices(physicalRowType, schema);
        // 推导数据汇的并行度
        final int sinkParallelism = deriveSinkParallelism(inputTransform, runtimeProvider);
        // 标记是否显式配置了并行度
        sinkParallelismConfigured = isParallelismConfigured(runtimeProvider);
        // 获取输入 Transformation 的并行度
        final int inputParallelism = inputTransform.getParallelism();
        // 检查输入变更日志是否仅包含插入操作
        final boolean inputInsertOnly = inputChangelogMode.containsOnly(RowKind.INSERT);
        // 检查是否有定义主键
        final boolean hasPk = primaryKeys.length > 0;

        // 如果输入不是仅插入模式，且数据汇并行度与输入并行度不同，且没有定义主键，则抛出异常
        if (!inputInsertOnly && sinkParallelism != inputParallelism && !hasPk) {
            throw new TableException(String.format(
                    "The sink for table '%s' has a configured parallelism of %s, while the input parallelism is %s. " +
                            "Since the configured parallelism is different from the input's parallelism and " +
                            "the changelog mode is not insert-only, a primary key is required but could not be found.",
                    tableSinkSpec.getContextResolvedTable().getIdentifier().asSummaryString(),
                    sinkParallelism,
                    inputParallelism));
        }

        // 只有当输入包含变更时才需要物化
        final boolean needMaterialization = !inputInsertOnly && upsertMaterialize;
        // 应用约束验证（可能是对输入数据的校验）
        Transformation<RowData> sinkTransform = applyConstraintValidations(inputTransform, config, physicalRowType);
        // 如果定义了主键，应用主键约束
        if (hasPk) {
            // 应用主键约束，可能涉及到数据的重新排序或分区
            sinkTransform =
                    applyKeyBy(
                            config,
                            classLoader,
                            sinkTransform,
                            primaryKeys,
                            sinkParallelism,
                            inputParallelism,
                            needMaterialization);
        }
        // 如果需要物化，应用物化逻辑
        if (needMaterialization) {
            // 应用物化逻辑，将更新和删除操作转换为插入和删除操作（如果目标系统不支持直接更新）
            sinkTransform =
                    applyUpsertMaterialize(
                            sinkTransform,
                            primaryKeys,
                            sinkParallelism,
                            config,
                            classLoader,
                            physicalRowType,
                            inputUpsertKey);
        }
        // 如果指定了目标行类型，应用行类型设置器
        Optional<RowKind> targetRowKind = getTargetRowKind();
        if (targetRowKind.isPresent()) {
            // 如果指定了目标行类型，则应用行类型设置器
            sinkTransform = applyRowKindSetter(sinkTransform, targetRowKind.get(), config);
        }
        // 最后，应用数据汇提供者来创建并返回最终的数据汇Transformation对象
        return (Transformation<Object>)
                applySinkProvider(
                        sinkTransform,
                        streamExecEnv, // Flink的流执行环境
                        runtimeProvider,// 数据汇的运行时提供者
                        rowtimeFieldIndex,// 行时间字段的索引
                        sinkParallelism,// 数据汇的并行度
                        config,// 执行节点的配置
                        classLoader);//类加载器
    }

    /**
     * 应用约束验证，自动负责配置或者报告错误，
     * 比如非空字段值与字段声明必须匹配（例如如果字段被声明为 NOT NULL，输入值不允许为 null）。
     *
     * @param inputTransform 输入的 Transformation，需要应用约束验证
     * @param config         执行节点配置
     * @param physicalRowType 物理行类型，包含字段信息
     * @return 应用约束验证后的 Transformation
     */
    private Transformation<RowData> applyConstraintValidations(
            Transformation<RowData> inputTransform,
            ExecNodeConfig config,
            RowType physicalRowType) {
        // 初始化约束执行器构建器
        final ConstraintEnforcer.Builder validatorBuilder = ConstraintEnforcer.newBuilder();
        // 获取物理行类型中所有字段的名称
        final String[] fieldNames = physicalRowType.getFieldNames().toArray(new String[0]);

        // 非空约束
        // 获取所有字段中不可为空的字段索引
        final int[] notNullFieldIndices = getNotNullFieldIndices(physicalRowType);
        if (notNullFieldIndices.length > 0) {
            final ExecutionConfigOptions.NotNullEnforcer notNullEnforcer = config.get(ExecutionConfigOptions.TABLE_EXEC_SINK_NOT_NULL_ENFORCER);
            // 将非空字段索引转换为字段名称列表
            final List<String> notNullFieldNames = Arrays.stream(notNullFieldIndices)
                    .mapToObj(idx -> fieldNames[idx])
                    .collect(Collectors.toList());
            // 添加非空约束到构建器
            validatorBuilder.addNotNullConstraint(notNullEnforcer, notNullFieldIndices, notNullFieldNames, fieldNames);
        }

        // 类型长度约束
        final ExecutionConfigOptions.TypeLengthEnforcer typeLengthEnforcer = config.get(ExecutionConfigOptions.TABLE_EXEC_SINK_TYPE_LENGTH_ENFORCER);

        // CHAR 类型长度约束
        final List<ConstraintEnforcer.FieldInfo> charFieldInfo = getFieldInfoForLengthEnforcer(physicalRowType, LengthEnforcerType.CHAR);
        if (!charFieldInfo.isEmpty()) {
            // 将字段信息中的索引转换为字段名称列表
            final List<String> charFieldNames = charFieldInfo.stream()
                    .map(cfi -> fieldNames[cfi.fieldIdx()])
                    .collect(Collectors.toList());
            // 添加 CHAR 类型长度约束到构建器
            validatorBuilder.addCharLengthConstraint(typeLengthEnforcer, charFieldInfo, charFieldNames, fieldNames);
        }

        // BINARY 类型长度约束
        final List<ConstraintEnforcer.FieldInfo> binaryFieldInfo = getFieldInfoForLengthEnforcer(physicalRowType, LengthEnforcerType.BINARY);
        if (!binaryFieldInfo.isEmpty()) {
            // 将字段信息中的索引转换为字段名称列表
            final List<String> binaryFieldNames = binaryFieldInfo.stream()
                    .map(cfi -> fieldNames[cfi.fieldIdx()])
                    .collect(Collectors.toList());
            // 添加 BINARY 类型长度约束到构建器
            validatorBuilder.addBinaryLengthConstraint(typeLengthEnforcer, binaryFieldInfo, binaryFieldNames, fieldNames);
        }

        // 构建最终的约束执行器
        ConstraintEnforcer constraintEnforcer = validatorBuilder.build();
        if (constraintEnforcer != null) {
            // 如果存在约束执行器，则创建新的 Transformation，并应用约束验证
            return ExecNodeUtil.createOneInputTransformation(
                    inputTransform,
                    createTransformationMeta(
                            CONSTRAINT_VALIDATOR_TRANSFORMATION,
                            constraintEnforcer.getOperatorName(),
                            "ConstraintEnforcer",
                            config),
                    constraintEnforcer,
                    getInputTypeInfo(),
                    inputTransform.getParallelism(),
                    false);
        } else {
            // 如果没有约束，则直接返回原始输入 Transformation
            return inputTransform;
        }
    }

    /**
     * 获取物理行类型中不可为空的字段索引数组。
     *
     * @param physicalType 物理行类型
     * @return 不可为空的字段索引数组
     */
    private int[] getNotNullFieldIndices(RowType physicalType) {
        return IntStream.range(0, physicalType.getFieldCount())
                .filter(pos -> !physicalType.getTypeAt(pos).isNullable())
                .toArray();
    }

    /**
     * 根据字段类型，返回对应的长度执行器所需的字段信息。
     * 用于确定字符串或二进制值是否需要修剪和/或填充。
     *
     * @param physicalType 物理行类型
     * @param enforcerType 长度执行器类型
     * @return 长度执行器所需字段信息的列表
     */
    private List<ConstraintEnforcer.FieldInfo> getFieldInfoForLengthEnforcer(
            RowType physicalType,
            LengthEnforcerType enforcerType) {
        LogicalTypeRoot staticType = null;
        LogicalTypeRoot variableType = null;
        int maxLength = 0;
        switch (enforcerType) {
            case CHAR:
                staticType = LogicalTypeRoot.CHAR;
                variableType = LogicalTypeRoot.VARCHAR;
                maxLength = CharType.MAX_LENGTH;
                break;
            case BINARY:
                staticType = LogicalTypeRoot.BINARY;
                variableType = LogicalTypeRoot.VARBINARY;
                maxLength = BinaryType.MAX_LENGTH;
        }
        final List<ConstraintEnforcer.FieldInfo> fieldsAndLengths = new ArrayList<>();
        for (int i = 0; i < physicalType.getFieldCount(); i++) {
            LogicalType type = physicalType.getTypeAt(i);
            boolean isStatic = type.is(staticType);
            // 检查是否需要进行长度验证
            if ((isStatic && (LogicalTypeChecks.getLength(type) < maxLength))
                    || (type.is(variableType) && (LogicalTypeChecks.getLength(type) < maxLength))) {
                fieldsAndLengths.add(new ConstraintEnforcer.FieldInfo(
                        i, LogicalTypeChecks.getLength(type), isStatic));
            } else if (isStatic) {
                // 仅需填充
                fieldsAndLengths.add(new ConstraintEnforcer.FieldInfo(i, null, isStatic));
            }
        }
        return fieldsAndLengths;
    }

    /**
     * 推导数据汇的并行度，假设数据汇运行时提供者实现了 {@link ParallelismProvider}。
     * 如果提供的并行度存在，则返回提供的并行度；否则使用输入 Transformation 的并行度。
     *
     * @param inputTransform 输入 Transformation
     * @param runtimeProvider 数据汇运行时提供者
     * @return 推导出的并行度
     */
    private int deriveSinkParallelism(
            Transformation<RowData> inputTransform,
            SinkRuntimeProvider runtimeProvider) {
        final int inputParallelism = inputTransform.getParallelism();
        if (isParallelismConfigured(runtimeProvider)) {
            int sinkParallelism = ((ParallelismProvider) runtimeProvider).getParallelism().get();
            if (sinkParallelism <= 0) {
                throw new TableException(String.format(
                        "Invalid configured parallelism %s for table '%s'.",
                        sinkParallelism,
                        tableSinkSpec.getContextResolvedTable().getIdentifier().asSummaryString()));
            }
            return sinkParallelism;
        } else {
            return inputParallelism;
        }
    }

    /**
     * 检查数据汇运行时提供者是否显式配置了并行度。
     *
     * @param runtimeProvider 数据汇运行时提供者
     * @return 是否显式配置了并行度
     */
    private boolean isParallelismConfigured(DynamicTableSink.SinkRuntimeProvider runtimeProvider) {
        return runtimeProvider instanceof ParallelismProvider
                && ((ParallelismProvider) runtimeProvider).getParallelism().isPresent();
    }

    /**
     * 应用主键分区转换，以保证变更日志消息的严格顺序。
     *
     * @param config 执行节点配置
     * @param classLoader 类加载器
     * @param inputTransform 输入 Transformation
     * @param primaryKeys 主键的索引数组
     * @param sinkParallelism 数据汇的并行度
     * @param inputParallelism 输入 Transformation 的并行度
     * @param needMaterialize 是否需要物化
     * @return 应用主键分区后的 Transformation
     */
    private Transformation<RowData> applyKeyBy(
            ExecNodeConfig config,
            ClassLoader classLoader,
            Transformation<RowData> inputTransform,
            int[] primaryKeys,
            int sinkParallelism,
            int inputParallelism,
            boolean needMaterialize) {
        final ExecutionConfigOptions.SinkKeyedShuffle sinkShuffleByPk = config.get(ExecutionConfigOptions.TABLE_EXEC_SINK_KEYED_SHUFFLE);
        boolean sinkKeyBy = false;
        switch (sinkShuffleByPk) {
            case NONE:
                // 不进行 any action
                break;
            case AUTO:
                // 当数据汇并行度与输入并行度不同时，且数据汇并行度不等于 1 时，启用主键分区
                sinkKeyBy = sinkParallelism != inputParallelism && sinkParallelism != 1;
                break;
            case FORCE:
                // 当数据汇并行度不等于 1 时，强制启用主键分区（单并行度不会导致数据乱序）
                sinkKeyBy = sinkParallelism != 1;
                break;
        }
        // 如果不需要主键分区且不需要物化，则直接返回输入 Transformation
        if (!sinkKeyBy && !needMaterialize) {
            return inputTransform;
        }

        // 创建 RowData key 选择器
        final RowDataKeySelector selector = KeySelectorUtil.getRowDataSelector(classLoader, primaryKeys, getInputTypeInfo());
        final KeyGroupStreamPartitioner<RowData, RowData> partitioner = new KeyGroupStreamPartitioner<>(
                selector, KeyGroupRangeAssignment.DEFAULT_LOWER_BOUND_MAX_PARALLELISM);
        // 创建分区转换
        Transformation<RowData> partitionedTransform = new PartitionTransformation<>(inputTransform, partitioner);
        // 填充 Transformation 元数据
        createTransformationMeta(PARTITIONER_TRANSFORMATION, "Partitioner", "Partitioner", config)
                .fill(partitionedTransform);
        partitionedTransform.setParallelism(sinkParallelism, sinkParallelismConfigured);
        return partitionedTransform;
    }

    /**
     * 应用物化逻辑，将更新和删除操作转换为插入或删除操作。
     *
     * @param inputTransform 输入 Transformation
     * @param primaryKeys 主键索引数组
     * @param sinkParallelism 数据汇并行度
     * @param config 执行节点配置
     * @param classLoader 类加载器
     * @param physicalRowType 物理行类型
     * @param inputUpsertKey 输入数据的更新键
     * @return 应用物化后的 Transformation
     */
    protected abstract Transformation<RowData> applyUpsertMaterialize(
            Transformation<RowData> inputTransform,
            int[] primaryKeys,
            int sinkParallelism,
            ExecNodeConfig config,
            ClassLoader classLoader,
            RowType physicalRowType,
            int[] inputUpsertKey);

    /**
     * 应用行类型设置器，设置目标行类型。
     *
     * @param inputTransform 输入 Transformation
     * @param rowKind 目标行类型
     * @param config 执行节点配置
     * @return 应用行类型设置器后的 Transformation
     */
    private Transformation<RowData> applyRowKindSetter(
            Transformation<RowData> inputTransform,
            RowKind rowKind,
            ExecNodeConfig config) {
        return ExecNodeUtil.createOneInputTransformation(
                inputTransform,
                createTransformationMeta(
                        ROW_KIND_SETTER,
                        String.format("RowKindSetter(TargetRowKind=[%s])", rowKind),
                        "RowKindSetter",
                        config),
                new RowKindSetter(rowKind),
                inputTransform.getOutputType(),
                inputTransform.getParallelism(),
                false);
    }

    /**
     * 根据数据汇运行时提供者类型，应用数据汇提供者逻辑，生成最终的 Transformation。
     *
     * @param inputTransform 输入 Transformation
     * @param env Flink 的流执行环境
     * @param runtimeProvider 数据汇运行时提供者
     * @param rowtimeFieldIndex 行时间字段的索引
     * @param sinkParallelism 数据汇并行度
     * @param config 执行节点配置
     * @param classLoader 类加载器
     * @return 应用数据汇提供者后的 Transformation
     */
    private Transformation<?> applySinkProvider(
            Transformation<RowData> inputTransform,
            StreamExecutionEnvironment env,
            SinkRuntimeProvider runtimeProvider,
            int rowtimeFieldIndex,
            int sinkParallelism,
            ExecNodeConfig config,
            ClassLoader classLoader) {
        try (TemporaryClassLoaderContext ignored = TemporaryClassLoaderContext.of(classLoader)) {

            // 创建 TransformationMetadata，用于存储转换的元数据
            TransformationMetadata sinkMeta = createTransformationMeta(SINK_TRANSFORMATION, config);

            // 根据运行时提供者类型生成 Transformation
            if (runtimeProvider instanceof DataStreamSinkProvider) {
                // 对于 DataStreamSinkProvider，应用时间戳转换并生成 DataStream
                Transformation<RowData> sinkTransformation = applyRowtimeTransformation(
                        inputTransform, rowtimeFieldIndex, sinkParallelism, config);
                final DataStream<RowData> dataStream = new DataStream<>(env, sinkTransformation);
                final DataStreamSinkProvider provider = (DataStreamSinkProvider) runtimeProvider;
                return provider.consumeDataStream(createProviderContext(config), dataStream).getTransformation();
            } else if (runtimeProvider instanceof TransformationSinkProvider) {
                // 对于 TransformationSinkProvider，直接创建 Transformation
                final TransformationSinkProvider provider = (TransformationSinkProvider) runtimeProvider;
                return provider.createTransformation(
                        new TransformationSinkProvider.Context() {
                            @Override
                            public Transformation<RowData> getInputTransformation() {
                                return inputTransform;
                            }

                            @Override
                            public int getRowtimeIndex() {
                                return rowtimeFieldIndex;
                            }

                            @Override
                            public Optional<String> generateUid(String name) {
                                return createProviderContext(config).generateUid(name);
                            }
                        });
            } else if (runtimeProvider instanceof SinkFunctionProvider) {
                // 对于 SinkFunctionProvider，创建 SinkFunction 并生成 Transformation
                final SinkFunction<RowData> sinkFunction = ((SinkFunctionProvider) runtimeProvider).createSinkFunction();
                return createSinkFunctionTransformation(sinkFunction, env, inputTransform, rowtimeFieldIndex, sinkMeta, sinkParallelism);
            } else if (runtimeProvider instanceof OutputFormatProvider) {
                // 对于 OutputFormatProvider，创建 OutputFormat 并生成 SinkFunctionTransformation
                OutputFormat<RowData> outputFormat = ((OutputFormatProvider) runtimeProvider).createOutputFormat();
                final SinkFunction<RowData> sinkFunction = new OutputFormatSinkFunction<>(outputFormat);
                return createSinkFunctionTransformation(sinkFunction, env, inputTransform, rowtimeFieldIndex, sinkMeta, sinkParallelism);
            } else if (runtimeProvider instanceof SinkProvider) {
                // 对于 SinkProvider，生成 DataStreamSink 的 Transformation
                Transformation<RowData> sinkTransformation = applyRowtimeTransformation(
                        inputTransform, rowtimeFieldIndex, sinkParallelism, config);
                final DataStream<RowData> dataStream = new DataStream<>(env, sinkTransformation);
                final Transformation<?> transformation = DataStreamSink.forSinkV1(
                        dataStream, ((SinkProvider) runtimeProvider).createSink(),
                        CustomSinkOperatorUidHashes.DEFAULT).getTransformation();
                transformation.setParallelism(sinkParallelism, sinkParallelismConfigured);
                sinkMeta.fill(transformation);
                return transformation;
            } else if (runtimeProvider instanceof SinkV2Provider) {
                // 对于 SinkV2Provider，生成 DataStreamSink 的 Transformation
                Transformation<RowData> sinkTransformation = applyRowtimeTransformation(
                        inputTransform, rowtimeFieldIndex, sinkParallelism, config);
                final DataStream<RowData> dataStream = new DataStream<>(env, sinkTransformation);
                final Transformation<?> transformation = DataStreamSink.forSink(
                        dataStream, ((SinkV2Provider) runtimeProvider).createSink(),
                        CustomSinkOperatorUidHashes.DEFAULT).getTransformation();
                transformation.setParallelism(sinkParallelism, sinkParallelismConfigured);
                sinkMeta.fill(transformation);
                return transformation;
            } else {
                throw new TableException("Unsupported sink runtime provider.");
            }
        }
    }

    /**
     * 创建数据汇提供者上下文，用于生成唯一 ID。
     *
     * @param config 执行节点配置
     * @return 提供者上下文
     */
    private ProviderContext createProviderContext(ExecNodeConfig config) {
        return name -> {
            if (this instanceof StreamExecNode && config.shouldSetUid()) {
                return Optional.of(createTransformationUid(name, config));
            }
            return Optional.empty();
        };
    }

    /**
     * 创建 SinkFunction 的 Transformation。
     *
     * @param sinkFunction SinkFunction，用于处理数据
     * @param env Flink 的流执行环境
     * @param inputTransformation 输入 Transformation
     * @param rowtimeFieldIndex 行时间字段的索引
     * @param transformationMetadata TransformationMetadata，包含元数据信息
     * @param sinkParallelism 数据汇并行度
     * @return 创建的 Transformation
     */
    private Transformation<?> createSinkFunctionTransformation(
            SinkFunction<RowData> sinkFunction,
            StreamExecutionEnvironment env,
            Transformation<RowData> inputTransformation,
            int rowtimeFieldIndex,
            TransformationMetadata transformationMetadata,
            int sinkParallelism) {
        final SinkOperator operator = new SinkOperator(env.clean(sinkFunction), rowtimeFieldIndex);

        // 如果 SinkFunction 支持输入类型配置，则设置输入类型
        if (sinkFunction instanceof InputTypeConfigurable) {
            ((InputTypeConfigurable) sinkFunction).setInputType(getInputTypeInfo(), env.getConfig());
        }

        // 创建 Transformation
        final Transformation<?> transformation = new LegacySinkTransformation<>(
                inputTransformation,
                transformationMetadata.getName(),
                SimpleOperatorFactory.of(operator),
                sinkParallelism,
                sinkParallelismConfigured);
        transformationMetadata.fill(transformation);
        return transformation;
    }

    /**
     * 应用时间戳插入转换，为每个流记录设置时间戳。
     *
     * @param inputTransform 输入 Transformation
     * @param rowtimeFieldIndex 行时间字段的索引
     * @param sinkParallelism 数据汇并行度
     * @param config 执行节点配置
     * @return 应用时间戳插入后的 Transformation
     */
    private Transformation<RowData> applyRowtimeTransformation(
            Transformation<RowData> inputTransform,
            int rowtimeFieldIndex,
            int sinkParallelism,
            ExecNodeConfig config) {
        // 如果没有行时间字段索引，则直接返回输入 Transformation
        if (rowtimeFieldIndex == -1) {
            return inputTransform;
        }
        return ExecNodeUtil.createOneInputTransformation(
                inputTransform,
                createTransformationMeta(
                        TIMESTAMP_INSERTER_TRANSFORMATION,
                        String.format("StreamRecordTimestampInserter(rowtime field: %s)", rowtimeFieldIndex),
                        "StreamRecordTimestampInserter",
                        config),
                new StreamRecordTimestampInserter(rowtimeFieldIndex),
                inputTransform.getOutputType(),
                sinkParallelism,
                sinkParallelismConfigured);
    }

    /**
     * 获取输入类型信息，包含数据类型的逻辑信息。
     *
     * @return 输入类型信息
     */
    private InternalTypeInfo<RowData> getInputTypeInfo() {
        return InternalTypeInfo.of(getInputEdges().get(0).getOutputType());
    }

    /**
     * 获取主键的索引数组，用于数据汇的主键分区等操作。
     *
     * @param sinkRowType 数据汇的行类型
     * @param schema 解析后的 Schema
     * @return 主键的索引数组
     */
    protected int[] getPrimaryKeyIndices(RowType sinkRowType, ResolvedSchema schema) {
        return schema.getPrimaryKey()
                .map(k -> k.getColumns().stream().mapToInt(sinkRowType::getFieldIndex).toArray())
                .orElse(new int[0]);
    }

    /**
     * 获取物理行类型，包含 Schema 的物理列信息。
     *
     * @param schema 解析后的 Schema
     * @return 物理行类型
     */
    protected RowType getPhysicalRowType(ResolvedSchema schema) {
        return (RowType) schema.toPhysicalRowDataType().getLogicalType();
    }

    // 长度执行器类型枚举
    private enum LengthEnforcerType {
        CHAR,
        BINARY
    }

    /**
     * 获取目标行类型，如果输入行类型是 INSERT，则返回对应的变更日志行类型。
     * 如果不需要变更，则返回空。
     *
     * @return 目标行类型，如果存在则非空
     */
    private Optional<RowKind> getTargetRowKind() {
        if (tableSinkSpec.getSinkAbilities() != null) {
            for (SinkAbilitySpec sinkAbilitySpec : tableSinkSpec.getSinkAbilities()) {
                if (sinkAbilitySpec instanceof RowLevelDeleteSpec) {
                    RowLevelDeleteSpec deleteSpec = (RowLevelDeleteSpec) sinkAbilitySpec;
                    if (deleteSpec.getRowLevelDeleteMode() == SupportsRowLevelDelete.RowLevelDeleteMode.DELETED_ROWS) {
                        return Optional.of(RowKind.DELETE);
                    }
                } else if (sinkAbilitySpec instanceof RowLevelUpdateSpec) {
                    RowLevelUpdateSpec updateSpec = (RowLevelUpdateSpec) sinkAbilitySpec;
                    if (updateSpec.getRowLevelUpdateMode() == SupportsRowLevelUpdate.RowLevelUpdateMode.UPDATED_ROWS) {
                        return Optional.of(RowKind.UPDATE_AFTER);
                    }
                }
            }
        }
        return Optional.empty();
    }
}
