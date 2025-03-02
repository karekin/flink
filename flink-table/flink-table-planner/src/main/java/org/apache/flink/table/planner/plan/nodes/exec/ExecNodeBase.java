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

package org.apache.flink.table.planner.plan.nodes.exec;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.runtime.partitioner.GlobalPartitioner;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.fusion.OpFusionCodegenSpecGenerator;
import org.apache.flink.table.planner.plan.nodes.exec.serde.ConfigurationJsonSerializerFilter;
import org.apache.flink.table.planner.plan.nodes.exec.utils.TransformationMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.visitor.ExecNodeVisitor;
import org.apache.flink.table.planner.plan.utils.ExecNodeMetadataUtil;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JacksonInject;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * 所有 {@link ExecNode} 的基类。
 *
 * @param <T> 此节点返回元素的类型。
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class ExecNodeBase<T> implements ExecNode<T> {

    /**
     * 该标志的默认值为 false。其他情况必须通过 {@link #setCompiled(boolean)} 设置此标志。
     * 这是为了避免因复杂构造函数过载而导致所有 {@link ExecNode} 出现构造函数参数的问题。
     * 但在反序列化时，该标志始终设置为 true。
     */
    @JacksonInject("isDeserialize")
    private boolean isCompiled;

    private final String description; // 节点的描述信息
    private final LogicalType outputType; // 输出的数据类型
    private final List<InputProperty> inputProperties; // 输入属性列表

    private List<ExecEdge> inputEdges; // 输入边列表
    private transient Transformation<T> transformation; // 转换为 Flink 的 Transformation

    private @Nullable transient OpFusionCodegenSpecGenerator fusionCodegenSpecGenerator; // 算子融合代码生成规范生成器
    /** 从 JSON 计划中反序列化获取的上下文信息（ID、名称、版本）。 */
    @JsonProperty(value = FIELD_NAME_TYPE, access = JsonProperty.Access.WRITE_ONLY)
    private final ExecNodeContext context;

    /**
     * 在 JSON 计划序列化过程中，从 {@link ExecNodeMetadata} 注解中检索默认上下文。
     */
    @JsonProperty(value = FIELD_NAME_TYPE, access = JsonProperty.Access.READ_ONLY, index = 1)
    protected final ExecNodeContext getContextFromAnnotation() {
        // 如果已编译，则返回 context，否则创建一个新的上下文并设置 ID
        return isCompiled ? context : ExecNodeContext.newContext(this.getClass()).withId(getId());
    }

    @JsonProperty(value = FIELD_NAME_CONFIGURATION, access = JsonProperty.Access.WRITE_ONLY)
    private final ReadableConfig persistedConfig; // 保存的配置

    @JsonProperty(
            value = FIELD_NAME_CONFIGURATION,
            access = JsonProperty.Access.READ_ONLY,
            index = 2)
    // 自定义筛选器，如果未使用消耗选项，则从节点配置中排除
    @JsonInclude(
            value = JsonInclude.Include.CUSTOM,
            valueFilter = ConfigurationJsonSerializerFilter.class)
    public ReadableConfig getPersistedConfig() {
        return persistedConfig;
    }

    /**
     * 构造一个执行节点基类。
     *
     * @param id                    节点 ID
     * @param context               执行节点上下文
     * @param persistedConfig       保存的配置，可能为 null
     * @param inputProperties       输入属性列表
     * @param outputType            输出数据类型
     * @param description           节点的描述
     */
    protected ExecNodeBase(
            int id,
            ExecNodeContext context,
            ReadableConfig persistedConfig,
            List<InputProperty> inputProperties,
            LogicalType outputType,
            String description) {
        this.context = checkNotNull(context).withId(id); // 初始化上下文并设置 ID
        this.persistedConfig = persistedConfig == null ? new Configuration() : persistedConfig; // 初始化配置
        this.inputProperties = checkNotNull(inputProperties); // 初始化输入属性
        this.outputType = checkNotNull(outputType); // 初始化输出数据类型
        this.description = checkNotNull(description); // 初始化描述
    }

    @Override
    public final int getId() {
        return context.getId(); // 返回节点 ID
    }

    @Override
    public String getDescription() {
        return description; // 返回节点描述
    }

    @Override
    public LogicalType getOutputType() {
        return outputType; // 返回输出数据类型
    }

    @Override
    public List<InputProperty> getInputProperties() {
        return inputProperties; // 返回输入属性列表
    }

    @Override
    public List<ExecEdge> getInputEdges() {
        return checkNotNull(
                inputEdges,
                "inputEdges should not be null, please call `setInputEdges(List<ExecEdge>)` first."); // 返回输入边列表
    }

    @Override
    public void setInputEdges(List<ExecEdge> inputEdges) {
        checkNotNull(inputEdges, "inputEdges should not be null."); // 输入边不能为空
        this.inputEdges = new ArrayList<>(inputEdges); // 设置输入边列表
    }

    @Override
    public void replaceInputEdge(int index, ExecEdge newInputEdge) {
        List<ExecEdge> edges = getInputEdges(); // 获取输入边列表
        checkArgument(index >= 0 && index < edges.size(), "Invalid index: " + index); // 索引必须有效
        edges.set(index, newInputEdge); // 替换指定位置的输入边
    }

    /**
     * 将 ExecNode 转换为 Flink 的 Transformation。
     */
    @Override
    public final Transformation<T> translateToPlan(Planner planner) {
        if (transformation == null) {
            // 如果 transformation 未初始化，则进行初始化
            transformation = translateToPlanInternal((PlannerBase) planner,
                    ExecNodeConfig.of(((PlannerBase) planner).getTableConfig(),
                            persistedConfig,
                            isCompiled));
            // 如果当前对象是 SingleTransformationTranslator 的实例，并且输入包含单例，则设置并行度为 1
            if (this instanceof SingleTransformationTranslator) {
                if (inputsContainSingleton(transformation)) {
                    transformation.setParallelism(1);
                    transformation.setMaxParallelism(1);
                }
            }
        }
        return transformation; // 返回转换后的 Transformation
    }

    @Override
    public void accept(ExecNodeVisitor visitor) {
        visitor.visit(this); // 接收执行节点访问者
    }

    @Override
    public void setCompiled(boolean compiled) {
        isCompiled = compiled; // 设置编译状态
    }

    /**
     * 将此节点转换为 Flink 的 Transformation。
     *
     * @param planner   PlannerBase 类型的规划器
     * @param config    每个 ExecNode 的配置，包含从不同层合并的配置，所有实现此方法的节点都应使用此配置，而不是从 planner 中检索配置。
     *                  有关详细信息，请参阅 {@link ExecNodeConfig}。
     * @return 转换后的 Transformation 对象
     */
    protected abstract Transformation<T> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config);

    private boolean inputsContainSingleton(Transformation<T> transformation) {
        return inputsContainSingleton()
                || transformation.getInputs().stream()
                .anyMatch(
                        input ->
                                input instanceof PartitionTransformation
                                        && ((PartitionTransformation<?>) input)
                                        .getPartitioner()
                                        instanceof GlobalPartitioner);
    }

    /** 是否需要单例分布。 */
    protected boolean inputsContainSingleton() {
        return getInputProperties().stream()
                .anyMatch(
                        p ->
                                p.getRequiredDistribution().getType()
                                        == InputProperty.DistributionType.SINGLETON);
    }

    @JsonIgnore
    protected String getSimplifiedName() {
        // 返回简化的类名
        return getClass().getSimpleName().replace("StreamExec", "").replace("BatchExec", "");
    }

    protected String createTransformationUid(String operatorName, ExecNodeConfig config) {
        // 根据上下文和配置生成唯一的 Uid
        return context.generateUid(operatorName, config);
    }

    protected String createTransformationName(ReadableConfig config) {
        // 创建转换的名称
        return createFormattedTransformationName(getDescription(), getSimplifiedName(), config);
    }

    protected String createTransformationDescription(ReadableConfig config) {
        // 创建转换的描述
        return createFormattedTransformationDescription(getDescription(), config);
    }

    /**
     * 创建转换的元数据。
     *
     * @param operatorName 操作符的名称，用于生成 Uid
     * @param config       执行节点配置
     * @return 转换元数据对象
     */
    protected TransformationMetadata createTransformationMeta(
            String operatorName, ExecNodeConfig config) {
        if (ExecNodeMetadataUtil.isUnsupported(this.getClass()) || !config.shouldSetUid()) {
            return new TransformationMetadata(
                    createTransformationName(config), createTransformationDescription(config));
        } else {
            return new TransformationMetadata(
                    createTransformationUid(operatorName, config),
                    createTransformationName(config),
                    createTransformationDescription(config));
        }
    }

    protected TransformationMetadata createTransformationMeta(
            String operatorName, String detailName, String simplifiedName, ExecNodeConfig config) {
        final String name = createFormattedTransformationName(detailName, simplifiedName, config);
        final String desc = createFormattedTransformationDescription(detailName, config);
        if (ExecNodeMetadataUtil.isUnsupported(this.getClass()) || !config.shouldSetUid()) {
            return new TransformationMetadata(name, desc);
        } else {
            return new TransformationMetadata(
                    createTransformationUid(operatorName, config), name, desc);
        }
    }

    protected String createFormattedTransformationDescription(
            String description, ReadableConfig config) {
        if (config.get(ExecutionConfigOptions.TABLE_EXEC_SIMPLIFY_OPERATOR_NAME_ENABLED)) {
            return String.format("[%d]:%s", getId(), description);
        }
        return description;
    }

    protected String createFormattedTransformationName(
            String detailName, String simplifiedName, ReadableConfig config) {
        if (config.get(ExecutionConfigOptions.TABLE_EXEC_SIMPLIFY_OPERATOR_NAME_ENABLED)) {
            return String.format("%s[%d]", simplifiedName, getId());
        }
        return detailName;
    }

    @VisibleForTesting
    @JsonIgnore
    public Transformation<T> getTransformation() {
        return this.transformation; // 返回转换后的 Transformation
    }

    @Override
    public boolean supportFusionCodegen() {
        return false; // 默认不支持算子融合代码生成
    }

    @Override
    public OpFusionCodegenSpecGenerator translateToFusionCodegenSpec(
            Planner planner, CodeGeneratorContext parentCtx) {
        if (fusionCodegenSpecGenerator == null) {
            fusionCodegenSpecGenerator =
                    translateToFusionCodegenSpecInternal(
                            (PlannerBase) planner,
                            ExecNodeConfig.of(
                                    ((PlannerBase) planner).getTableConfig(),
                                    persistedConfig,
                                    isCompiled),
                            parentCtx);
        }
        return fusionCodegenSpecGenerator;
    }

    /**
     * 将此节点转换为算子融合代码生成规范生成器。
     *
     * @param planner   PlannerBase 类型的规划器
     * @param config    每个 ExecNode 的配置，包含从不同层合并的配置，所有实现此方法的节点都应使用此配置，而不是从 planner 中检索配置。
     *                  有关详细信息，请参阅 {@link ExecNodeConfig}。
     * @param parentCtx 父级代码生成上下文
     * @return 转换后的 OpFusionCodegenSpecGenerator 对象
     */
    protected OpFusionCodegenSpecGenerator translateToFusionCodegenSpecInternal(
            PlannerBase planner, ExecNodeConfig config, CodeGeneratorContext parentCtx) {
        throw new TableException("This ExecNode doesn't support operator fusion codegen now.");
    }
}
