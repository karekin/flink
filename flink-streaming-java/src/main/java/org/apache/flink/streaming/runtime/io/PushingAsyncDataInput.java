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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.io.AvailabilityProvider;
import org.apache.flink.runtime.io.PullingAsyncDataInput;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.RecordAttributes;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;

/**
 * {@link PullingAsyncDataInput} 的变体，通过 {@link #emitNext(DataOutput)} 方法以统一方式处理网络输入和源输入，
 * 而不是像 {@link PullingAsyncDataInput#pollNext()} 那样返回 {@code Optional.empty()}。
 *
 * 此接口用于处理非阻塞的数据输入，适用于需要以非阻塞方式从数据源拉取数据并在异步上下文中处理的场景。
 * 异步数据输入可以在不阻塞主线程的情况下处理诸如网络请求或磁盘读取等操作，从而提高系统的响应能力和吞吐量。
 */

@Internal
public interface PushingAsyncDataInput<T> extends AvailabilityProvider {

    /**
     * 从当前数据输入中将元素推送到输出，并返回输入状态，以指示当前输入是否有更多可用数据。
     *
     * <p>此方法应该是非阻塞的。
     * <p>这是一个关键方法，用于从数据源中提取数据并将其推送到处理管道中。
     * 例如，在流处理系统中，数据源可能是一个网络服务或一个文件系统。
     * 当数据源有新数据可用时，此方法会被调用，将数据推送到输出。
     * 非阻塞的特性意味着此方法不会等待数据准备好，而是立即返回，无论数据是否可用。
     * 这使得系统能够更高效地处理高吞吐量的数据流。
     *
     * @param output 数据输出对象，用于接收和处理数据。
     * @return 数据输入状态，表示是否有更多可用数据。
     * @throws Exception 如果在推送数据到输出时发生任何异常。
     */
    DataInputStatus emitNext(DataOutput<T> output) throws Exception;

    /**
     * 用于从数据输入中发出下一个元素的基本数据输出接口。
     *
     * <p>此接口提供了将数据发送到下游处理组件的方法，例如发送数据记录、水印（watermark）、延迟标记（latency marker）等。
     * 通过实现此接口，可以将数据从不同的数据源传输到处理管道中，进行进一步的处理和分析。
     *
     * @param <T> 数据流中封装的类型。
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 基本数据输出接口，用于从数据输入中发出下一个元素。
     */
    interface DataOutput<T> {

        void emitRecord(StreamRecord<T> streamRecord) throws Exception;

        void emitWatermark(Watermark watermark) throws Exception;

        void emitWatermarkStatus(WatermarkStatus watermarkStatus) throws Exception;

        void emitLatencyMarker(LatencyMarker latencyMarker) throws Exception;

        void emitRecordAttributes(RecordAttributes recordAttributes) throws Exception;
    }
}
