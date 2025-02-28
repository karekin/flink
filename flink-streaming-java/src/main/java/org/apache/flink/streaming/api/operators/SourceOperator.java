/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.eventtime.WatermarkAlignmentParams;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.apache.flink.runtime.io.AvailabilityProvider;
import org.apache.flink.runtime.io.network.api.StopMode;
import org.apache.flink.runtime.metrics.groups.InternalSourceReaderMetricGroup;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.OperatorEventHandler;
import org.apache.flink.runtime.source.event.AddSplitEvent;
import org.apache.flink.runtime.source.event.IsProcessingBacklogEvent;
import org.apache.flink.runtime.source.event.NoMoreSplitsEvent;
import org.apache.flink.runtime.source.event.ReaderRegistrationEvent;
import org.apache.flink.runtime.source.event.ReportedWatermarkEvent;
import org.apache.flink.runtime.source.event.RequestSplitEvent;
import org.apache.flink.runtime.source.event.SourceEventWrapper;
import org.apache.flink.runtime.source.event.WatermarkAlignmentEvent;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.source.TimestampsAndWatermarks;
import org.apache.flink.streaming.api.operators.util.SimpleVersionedListState;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.DataInputStatus;
import org.apache.flink.streaming.runtime.io.MultipleFuturesAvailabilityHelper;
import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput;
import org.apache.flink.streaming.runtime.streamrecord.RecordAttributesBuilder;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.tasks.StreamTask.CanEmitBatchOfRecordsChecker;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.UserCodeClassLoader;
import org.apache.flink.util.function.FunctionWithException;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.configuration.PipelineOptions.ALLOW_UNALIGNED_SOURCE_SPLITS;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * 基础源操作符，仅用于集成 FLIP-27 提出的源读取器（Source Reader）。它实现了 {@link PushingAsyncDataInput} 接口，
 * 该接口与运行时栈中的单输入处理方式天然兼容。
 *
 * <p><b>重要序列化注意事项：</b>SourceOperator 从 StreamOperator 继承了 {@link java.io.Serializable} 接口，但事实上它不是可序列化的。
 * 该操作符只能在 StreamTask 中通过其工厂实例化。
 *
 * @param <OUT> 操作符的输出类型。
 */
@Internal
public class SourceOperator<OUT, SplitT extends SourceSplit> extends AbstractStreamOperator<OUT>
        implements OperatorEventHandler,
        PushingAsyncDataInput<OUT>,
        TimestampsAndWatermarks.WatermarkUpdateListener {
    private static final long serialVersionUID = 1405537676017904695L;

    // 用于单测，定义了一个保存源读取器状态的 ListStateDescriptor
    static final ListStateDescriptor<byte[]> SPLITS_STATE_DESC =
            new ListStateDescriptor<>("SourceReaderState", BytePrimitiveArraySerializer.INSTANCE);

    /**
     * 源读取器的工厂。这是一个变通方案，因为当前 SourceReader 必须延迟初始化，这主要是因为读取器依赖的指标组是延迟初始化的。
     */
    private final FunctionWithException<SourceReaderContext, SourceReader<OUT, SplitT>, Exception>
            readerFactory;

    /**
     * 分裂序列化器，用于在将分裂类型存储到读取器状态之前对其进行序列化。
     */
    private final SimpleVersionedSerializer<SplitT> splitSerializer;

    /** 用于事件网关，操作符通过它与协调器进行通信。 */
    private final OperatorEventGateway operatorEventGateway;

    /** 时间戳和水印生成器的工厂。 */
    private final WatermarkStrategy<OUT> watermarkStrategy;

    private final WatermarkAlignmentParams watermarkAlignmentParams;

    /** Flth 配置。 */
    private final Configuration configuration;

    /**
     * 操作符运行所在机器的主机名，用于支持基于位置的工作分配。
     */
    private final String localHostname;

    /** 是否发出中间水印，还是仅在输入结束时发出一个最终水印。 */
    private final boolean emitProgressiveWatermarks;

    // ---- 延迟初始化的字段（这些字段是“热”字段） ----

    /** 执行大部分工作的源读取器。 */
    private SourceReader<OUT, SplitT> sourceReader;

    private ReaderOutput<OUT> currentMainOutput;

    private DataOutput<OUT> lastInvokedOutput;

    private long latestWatermark = Watermark.UNINITIALIZED.getTimestamp();

    private boolean idle = false;

    /** 保存当前分配的分裂状态。 */
    private ListState<SplitT> readerState;

    /**
     * 事件时间和水印逻辑，在此逻辑尚未完全形成时初始化某些方面。
     * 理想情况下，这应该在初始化时传入此操作符，但由于目前指标组仅在稍后存在，因此必须延迟初始化。
     */
    private TimestampsAndWatermarks<OUT> eventTimeLogic;

    /** 用于控制 {@link #emitNext(DataOutput)} 方法的行为的模式。 */
    private OperatingMode operatingMode;

    private final CompletableFuture<Void> finished = new CompletableFuture<>();
    private final SourceOperatorAvailabilityHelper availabilityHelper =
            new SourceOperatorAvailabilityHelper();

    private final List<SplitT> outputPendingSplits = new ArrayList<>();

    private int numSplits;
    private final Map<String, Long> splitCurrentWatermarks = new HashMap<>();
    private final Set<String> currentlyPausedSplits = new HashSet<>();

    private enum OperatingMode {
        READING,
        WAITING_FOR_ALIGNMENT,
        OUTPUT_NOT_INITIALIZED,
        SOURCE_DRAINED,
        SOURCE_STOPPED,
        DATA_FINISHED
    }

    private InternalSourceReaderMetricGroup sourceMetricGroup;

    private long currentMaxDesiredWatermark = Watermark.MAX_WATERMARK.getTimestamp();
    /** 只能在 {@link OperatingMode#WAITING_FOR_ALIGNMENT} 模式下未完成时有效。 */
    private CompletableFuture<Void> waitingForAlignmentFuture =
            CompletableFuture.completedFuture(null);

    private @Nullable LatencyMarkerEmitter<OUT> latencyMarkerEmitter;

    private final boolean allowUnalignedSourceSplits;

    private final CanEmitBatchOfRecordsChecker canEmitBatchOfRecords;

    public SourceOperator(
            FunctionWithException<SourceReaderContext, SourceReader<OUT, SplitT>, Exception>
                    readerFactory,
            OperatorEventGateway operatorEventGateway,
            SimpleVersionedSerializer<SplitT> splitSerializer,
            WatermarkStrategy<OUT> watermarkStrategy,
            ProcessingTimeService timeService,
            Configuration configuration,
            String localHostname,
            boolean emitProgressiveWatermarks,
            CanEmitBatchOfRecordsChecker canEmitBatchOfRecords) {

        this.readerFactory = checkNotNull(readerFactory);
        this.operatorEventGateway = checkNotNull(operatorEventGateway);
        this.splitSerializer = checkNotNull(splitSerializer);
        this.watermarkStrategy = checkNotNull(watermarkStrategy);
        this.processingTimeService = timeService;
        this.configuration = checkNotNull(configuration);
        this.localHostname = checkNotNull(localHostname);
        this.emitProgressiveWatermarks = emitProgressiveWatermarks;
        this.operatingMode = OperatingMode.OUTPUT_NOT_INITIALIZED;
        this.watermarkAlignmentParams = watermarkStrategy.getAlignmentParameters();
        this.allowUnalignedSourceSplits = configuration.get(ALLOW_UNALIGNED_SOURCE_SPLITS);
        this.canEmitBatchOfRecords = checkNotNull(canEmitBatchOfRecords);
    }

    @Override
    public void setup(
            StreamTask<?, ?> containingTask,
            StreamConfig config,
            Output<StreamRecord<OUT>> output) {
        super.setup(containingTask, config, output);
        initSourceMetricGroup();
    }

    @VisibleForTesting
    protected void initSourceMetricGroup() {
        sourceMetricGroup = InternalSourceReaderMetricGroup.wrap(getMetricGroup());
    }

    /**
     * 初始化读取器。该方法中的代码理想情况下应该发生在构造函数或甚至操作符工厂中。但由于延迟的指标初始化，它必须稍后在此处执行。
     * <p> explicitlt 调用该方法是一个可选的方法，可以比在 open() 中稍早地初始化读取器，如 {@link org.apache.flink.streaming.runtime.tasks.SourceOperatorStreamTask} 所需的那样。
     * <p>一旦任务设置时指标组可用，该代码就应移到构造函数中。
     */
    public void initReader() throws Exception {
        if (sourceReader != null) {
            return;
        }

        final int subtaskIndex = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();

        final SourceReaderContext context =
                new SourceReaderContext() {
                    @Override
                    public SourceReaderMetricGroup metricGroup() {
                        return sourceMetricGroup;
                    }

                    @Override
                    public Configuration getConfiguration() {
                        return configuration;
                    }

                    @Override
                    public String getLocalHostName() {
                        return localHostname;
                    }

                    @Override
                    public int getIndexOfSubtask() {
                        return subtaskIndex;
                    }

                    @Override
                    public void sendSplitRequest() {
                        operatorEventGateway.sendEventToCoordinator(
                                new RequestSplitEvent(getLocalHostName()));
                    }

                    @Override
                    public void sendSourceEventToCoordinator(SourceEvent event) {
                        operatorEventGateway.sendEventToCoordinator(new SourceEventWrapper(event));
                    }

                    @Override
                    public UserCodeClassLoader getUserCodeClassLoader() {
                        return new UserCodeClassLoader() {
                            @Override
                            public ClassLoader asClassLoader() {
                                return getRuntimeContext().getUserCodeClassLoader();
                            }

                            @Override
                            public void registerReleaseHookIfAbsent(
                                    String releaseHookName, Runnable releaseHook) {
                                getRuntimeContext()
                                        .registerUserCodeClassLoaderReleaseHookIfAbsent(
                                                releaseHookName, releaseHook);
                            }
                        };
                    }

                    @Override
                    public int currentParallelism() {
                        return getRuntimeContext().getTaskInfo().getNumberOfParallelSubtasks();
                    }
                };

        sourceReader = readerFactory.apply(context);
    }

    public InternalSourceReaderMetricGroup getSourceMetricGroup() {
        return sourceMetricGroup;
    }

    @Override
    public void open() throws Exception {
        initReader();

        // 在未来，当该操作符迁移到“急切初始化”操作符（StreamOperatorV2）时，应该在操作符构造时对其进行评估。
        if (emitProgressiveWatermarks) {
            eventTimeLogic =
                    TimestampsAndWatermarks.createProgressiveEventTimeLogic(
                            watermarkStrategy,
                            sourceMetricGroup,
                            getProcessingTimeService(),
                            getExecutionConfig().getAutoWatermarkInterval());
        } else {
            eventTimeLogic =
                    TimestampsAndWatermarks.createNoOpEventTimeLogic(
                            watermarkStrategy, sourceMetricGroup);
        }

        // 恢复状态（如必要）。
        final List<SplitT> splits = CollectionUtil.iterableToList(readerState.get());
        if (!splits.isEmpty()) {
            LOG.info("Restoring state for {} split(s) to reader.", splits.size());
            sourceReader.addSplits(splits);
        }

        // 将读取器注册到协调器。
        registerReader();

        sourceMetricGroup.idlingStarted();
        // 启动读取器，发送消息后启动。
        sourceReader.start();

        eventTimeLogic.startPeriodicWatermarkEmits();
    }

    @Override
    public void finish() throws Exception {
        stopInternalServices();
        super.finish();

        finished.complete(null);
    }

    private void stopInternalServices() {
        if (eventTimeLogic != null) {
            eventTimeLogic.stopPeriodicWatermarkEmits();
        }
        if (latencyMarkerEmitter != null) {
            latencyMarkerEmitter.close();
        }
    }

    public CompletableFuture<Void> stop(StopMode mode) {
        switch (operatingMode) {
            case WAITING_FOR_ALIGNMENT:
            case OUTPUT_NOT_INITIALIZED:
            case READING:
                this.operatingMode =
                        mode == StopMode.DRAIN
                                ? OperatingMode.SOURCE_DRAINED
                                : OperatingMode.SOURCE_STOPPED;
                availabilityHelper.forceStop();
                if (this.operatingMode == OperatingMode.SOURCE_STOPPED) {
                    stopInternalServices();
                    finished.complete(null);
                    return finished;
                }
                break;
        }
        return finished;
    }

    @Override
    public void close() throws Exception {
        if (sourceReader != null) {
            sourceReader.close();
        }
        super.close();
    }

    @Override
    public DataInputStatus emitNext(DataOutput<OUT> output) throws Exception {
        // 确保当前假设，因为某些类假设一个恒定的 output，在这种情况下，output 将更改为 FinishedDataOutput。
        assert lastInvokedOutput == output
                || lastInvokedOutput == null
                || this.operatingMode == OperatingMode.DATA_FINISHED;

        // 快速通过热门路径。如果没有这种快速通过（READING 在 switch/case 中处理），InputBenchmark.mapSink 表现出了性能下降。
        if (operatingMode != OperatingMode.READING) {
            return emitNextNotReading(output);
        }

        InputStatus status;
        do {
            status = sourceReader.pollNext(currentMainOutput);
        } while (status == InputStatus.MORE_AVAILABLE
                && canEmitBatchOfRecords.check()
                && !shouldWaitForAlignment());
        return convertToInternalStatus(status);
    }

    private DataInputStatus emitNextNotReading(DataOutput<OUT> output) throws Exception {
        switch (operatingMode) {
            case OUTPUT_NOT_INITIALIZED:
                if (watermarkAlignmentParams.isEnabled()) {
                    // 如果启用了水印对齐，应仅在此时包装输出。否则，这将引入小的性能退化（可能是由于额外的虚调用）。
                    processingTimeService.scheduleWithFixedDelay(
                            this::emitLatestWatermark,
                            watermarkAlignmentParams.getUpdateInterval(),
                            watermarkAlignmentParams.getUpdateInterval());
                }
                initializeMainOutput(output);
                return convertToInternalStatus(sourceReader.pollNext(currentMainOutput));
            case SOURCE_STOPPED:
                this.operatingMode = OperatingMode.DATA_FINISHED;
                sourceMetricGroup.idlingStarted();
                return DataInputStatus.STOPPED;
            case SOURCE_DRAINED:
                this.operatingMode = OperatingMode.DATA_FINISHED;
                sourceMetricGroup.idlingStarted();
                return DataInputStatus.END_OF_DATA;
            case DATA_FINISHED:
                sourceMetricGroup.idlingStarted();
                return DataInputStatus.END_OF_INPUT;
            case WAITING_FOR_ALIGNMENT:
                checkState(!waitingForAlignmentFuture.isDone());
                checkState(shouldWaitForAlignment());
                return convertToInternalStatus(InputStatus.NOTHING_AVAILABLE);
            case READING:
            default:
                throw new IllegalStateException("Unknown operating mode: " + operatingMode);
        }
    }

    private void initializeMainOutput(DataOutput<OUT> output) {
        currentMainOutput = eventTimeLogic.createMainOutput(output, this);
        initializeLatencyMarkerEmitter(output);
        lastInvokedOutput = output;
        // 为尚未初始化的 main output 的 splits 创建输出
        createOutputForSplits(outputPendingSplits);
        this.operatingMode = OperatingMode.READING;
    }

    private void initializeLatencyMarkerEmitter(DataOutput<OUT> output) {
        long latencyTrackingInterval =
                getExecutionConfig().isLatencyTrackingConfigured()
                        ? getExecutionConfig().getLatencyTrackingInterval()
                        : getContainingTask()
                        .getEnvironment()
                        .getTaskManagerInfo()
                        .getConfiguration()
                        .get(MetricOptions.LATENCY_INTERVAL);
        if (latencyTrackingInterval > 0) {
            latencyMarkerEmitter =
                    new LatencyMarkerEmitter<>(
                            getProcessingTimeService(),
                            output::emitLatencyMarker,
                            latencyTrackingInterval,
                            getOperatorID(),
                            getRuntimeContext().getTaskInfo().getIndexOfThisSubtask());
        }
    }

    private DataInputStatus convertToInternalStatus(InputStatus inputStatus) {
        switch (inputStatus) {
            case MORE_AVAILABLE:
                return DataInputStatus.MORE_AVAILABLE;
            case NOTHING_AVAILABLE:
                sourceMetricGroup.idlingStarted();
                return DataInputStatus.NOTHING_AVAILABLE;
            case END_OF_INPUT:
                this.operatingMode = OperatingMode.DATA_FINISHED;
                sourceMetricGroup.idlingStarted();
                return DataInputStatus.END_OF_DATA;
            default:
                throw new IllegalArgumentException("Unknown input status: " + inputStatus);
        }
    }

    private void emitLatestWatermark(long time) {
        checkState(currentMainOutput != null);
        if (latestWatermark == Watermark.UNINITIALIZED.getTimestamp()) {
            return;
        }
        operatorEventGateway.sendEventToCoordinator(
                new ReportedWatermarkEvent(
                        idle ? Watermark.MAX_WATERMARK.getTimestamp() : latestWatermark));
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        long checkpointId = context.getCheckpointId();
        LOG.debug("Taking a snapshot for checkpoint {}", checkpointId);
        readerState.update(sourceReader.snapshotState(checkpointId));
    }

    @Override
    public CompletableFuture<?> getAvailableFuture() {
        switch (operatingMode) {
            case WAITING_FOR_ALIGNMENT:
                return availabilityHelper.update(waitingForAlignmentFuture);
            case OUTPUT_NOT_INITIALIZED:
            case READING:
                return availabilityHelper.update(sourceReader.isAvailable());
            case SOURCE_STOPPED:
            case SOURCE_DRAINED:
            case DATA_FINISHED:
                return AvailabilityProvider.AVAILABLE;
            default:
                throw new IllegalStateException("Unknown operating mode: " + operatingMode);
        }
    }

    /**
     * 初始化状态的方法，通常用于在流处理任务（如 Flink 任务）启动时恢复或设置任务的状态。
     */
    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        // 调用父类的 initializeState 方法，进行任何必要的默认状态初始化或准备工作。
        super.initializeState(context);
        // 从 OperatorStateStore 中获取名为 SPLITS_STATE_DESC 的 ListState 对象，用于存储和恢复字节数组列表的状态。
        final ListState<byte[]> rawState =
                context.getOperatorStateStore().getListState(SPLITS_STATE_DESC);
        // 使用获取到的 rawState 和 splitSerializer（序列化器）创建一个 SimpleVersionedListState 对象。
        // 该对象封装了 rawState 并提供了一些额外的功能，如版本控制。
        // readerState 用于后续读取状态信息，它现在与 SPLITS_STATE_DESC 对应的状态绑定在一起。
        readerState = new SimpleVersionedListState<>(rawState, splitSerializer);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        super.notifyCheckpointComplete(checkpointId);
        sourceReader.notifyCheckpointComplete(checkpointId);
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        super.notifyCheckpointAborted(checkpointId);
        sourceReader.notifyCheckpointAborted(checkpointId);
    }

    @SuppressWarnings("unchecked")
    public void handleOperatorEvent(OperatorEvent event) {
        if (event instanceof WatermarkAlignmentEvent) {
            updateMaxDesiredWatermark((WatermarkAlignmentEvent) event);
            checkWatermarkAlignment();
            checkSplitWatermarkAlignment();
        } else if (event instanceof AddSplitEvent) {
            handleAddSplitsEvent(((AddSplitEvent<SplitT>) event));
        } else if (event instanceof SourceEventWrapper) {
            sourceReader.handleSourceEvents(((SourceEventWrapper) event).getSourceEvent());
        } else if (event instanceof NoMoreSplitsEvent) {
            sourceReader.notifyNoMoreSplits();
        } else if (event instanceof IsProcessingBacklogEvent) {
            if (eventTimeLogic != null) {
                eventTimeLogic.emitImmediateWatermark(System.currentTimeMillis());
            }
            output.emitRecordAttributes(
                    new RecordAttributesBuilder(Collections.emptyList())
                            .setBacklog(((IsProcessingBacklogEvent) event).isProcessingBacklog())
                            .build());
        } else {
            throw new IllegalStateException("Received unexpected operator event " + event);
        }
    }

    private void handleAddSplitsEvent(AddSplitEvent<SplitT> event) {
        try {
            List<SplitT> newSplits = event.splits(splitSerializer);
            numSplits += newSplits.size();
            if (operatingMode == OperatingMode.OUTPUT_NOT_INITIALIZED) {
                // 如果主输出尚未初始化，将到达的 splits 存储到 pending 列表中。
                // 在主输出准备就绪时，会为这些 splits 创建输出。
                outputPendingSplits.addAll(newSplits);
            } else {
                // 如果主输出已经初始化，则直接为这些 splits 创建输出
                createOutputForSplits(newSplits);
            }
            sourceReader.addSplits(newSplits);
        } catch (IOException e) {
            throw new FlinkRuntimeException("Failed to deserialize the splits.", e);
        }
    }

    private void createOutputForSplits(List<SplitT> newSplits) {
        for (SplitT split : newSplits) {
            currentMainOutput.createOutputForSplit(split.splitId());
        }
    }

    private void updateMaxDesiredWatermark(WatermarkAlignmentEvent event) {
        currentMaxDesiredWatermark = event.getMaxWatermark();
        sourceMetricGroup.updateMaxDesiredWatermark(currentMaxDesiredWatermark);
    }

    @Override
    public void updateIdle(boolean isIdle) {
        this.idle = isIdle;
    }

    @Override
    public void updateCurrentEffectiveWatermark(long watermark) {
        latestWatermark = watermark;
        checkWatermarkAlignment();
    }

    @Override
    public void updateCurrentSplitWatermark(String splitId, long watermark) {
        splitCurrentWatermarks.put(splitId, watermark);
        if (numSplits > 1
                && watermark > currentMaxDesiredWatermark
                && !currentlyPausedSplits.contains(splitId)) {
            pauseOrResumeSplits(Collections.singletonList(splitId), Collections.emptyList());
            currentlyPausedSplits.add(splitId);
        }
    }

    /**
     * <p>找出了需要被暂停的超过当前最大水印的 splits。</p>
     * <p>同时，恢复那些已经被暂停且全局水印已经追上的 splits。</p>
     * <p>注意：只有在 splits 数量大于 1 时才起作用，否则不执行任何操作。</p>
     */
    private void checkSplitWatermarkAlignment() {
        if (numSplits <= 1) {
            // 单个 split 无法超过此操作符实例分配的其他 splits，对源看起来是处理的。
            return;
        }
        Collection<String> splitsToPause = new ArrayList<>();
        Collection<String> splitsToResume = new ArrayList<>();
        splitCurrentWatermarks.forEach(
                (splitId, splitWatermark) -> {
                    if (splitWatermark > currentMaxDesiredWatermark) {
                        splitsToPause.add(splitId);
                    } else if (currentlyPausedSplits.contains(splitId)) {
                        splitsToResume.add(splitId);
                    }
                });
        splitsToPause.removeAll(currentlyPausedSplits);
        if (!splitsToPause.isEmpty() || !splitsToResume.isEmpty()) {
            pauseOrResumeSplits(splitsToPause, splitsToResume);
            currentlyPausedSplits.addAll(splitsToPause);
            splitsToResume.forEach(currentlyPausedSplits::remove);
        }
    }

    private void pauseOrResumeSplits(
            Collection<String> splitsToPause, Collection<String> splitsToResume) {
        try {
            sourceReader.pauseOrResumeSplits(splitsToPause, splitsToResume);
        } catch (UnsupportedOperationException e) {
            if (!allowUnalignedSourceSplits) {
                throw e;
            }
        }
    }

    private void checkWatermarkAlignment() {
        if (operatingMode == OperatingMode.READING) {
            checkState(waitingForAlignmentFuture.isDone());
            if (shouldWaitForAlignment()) {
                operatingMode = OperatingMode.WAITING_FOR_ALIGNMENT;
                waitingForAlignmentFuture = new CompletableFuture<>();
            }
        } else if (operatingMode == OperatingMode.WAITING_FOR_ALIGNMENT) {
            checkState(!waitingForAlignmentFuture.isDone());
            if (!shouldWaitForAlignment()) {
                operatingMode = OperatingMode.READING;
                waitingForAlignmentFuture.complete(null);
            }
        }
    }

    private boolean shouldWaitForAlignment() {
        return currentMaxDesiredWatermark < latestWatermark;
    }

    private void registerReader() {
        operatorEventGateway.sendEventToCoordinator(
                new ReaderRegistrationEvent(
                        getRuntimeContext().getTaskInfo().getIndexOfThisSubtask(), localHostname));
    }

    // ------------ 方法用于单元测试 UNIT TESTS ------------

    @VisibleForTesting
    public SourceReader<OUT, SplitT> getSourceReader() {
        return sourceReader;
    }

    @VisibleForTesting
    ListState<SplitT> getReaderState() {
        return readerState;
    }

    private static class SourceOperatorAvailabilityHelper {
        private final CompletableFuture<Void> forcedStopFuture = new CompletableFuture<>();
        private final MultipleFuturesAvailabilityHelper availabilityHelper;

        private SourceOperatorAvailabilityHelper() {
            availabilityHelper = new MultipleFuturesAvailabilityHelper(2);
            availabilityHelper.anyOf(0, forcedStopFuture);
        }

        public CompletableFuture<?> update(CompletableFuture<Void> sourceReaderFuture) {
            if (sourceReaderFuture == AvailabilityProvider.AVAILABLE
                    || sourceReaderFuture.isDone()) {
                return AvailabilityProvider.AVAILABLE;
            }
            availabilityHelper.resetToUnAvailable();
            availabilityHelper.anyOf(0, forcedStopFuture);
            availabilityHelper.anyOf(1, sourceReaderFuture);
            return availabilityHelper.getAvailableFuture();
        }

        public void forceStop() {
            forcedStopFuture.complete(null);
        }
    }
}
