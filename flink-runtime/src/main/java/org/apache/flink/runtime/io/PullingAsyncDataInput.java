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

package org.apache.flink.runtime.io;

import org.apache.flink.annotation.Internal;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * 定义了一些用于异步和非阻塞数据轮询的核心方法的接口。
 *
 * <p>为了实现最高效的使用，用户应该调用 {@link #pollNext()}，直到它返回没有更多元素可用。
 * 如果发生这种情况，用户应检查输入是否 {@link #isFinished()}。如果没有完成，用户应该等待
 * {@link #getAvailableFuture()} 的 {@link CompletableFuture} 完成。例如：
 *
 * <pre>{@code
 * AsyncDataInput<T> input = ...;
 * while (!input.isFinished()) {
 * 	Optional<T> next;
 *
 * 	while (true) {
 * 		next = input.pollNext();
 * 		if (!next.isPresent()) {
 * 			break;
 * 		}
 * 		// 对下一个元素进行处理
 * 	}
 *
 * 	input.getAvailableFuture().get();
 * }
 * }</pre>
 */
@Internal
public interface PullingAsyncDataInput<T> extends AvailabilityProvider {

    /**
     * 轮询下一个元素。此方法应该是非阻塞的。
     *
     * @return 如果没有数据要返回或 {@link #isFinished()} 返回 true，则返回 {@code Optional.empty()}。
     * 否则返回 {@code Optional.of(element)}。
     * @throws Exception 如果在轮询下一个元素时发生异常。
     */
    Optional<T> pollNext() throws Exception;

    /**
     * 判断是否已经完成且例如达到了输入的结束，否则返回 false。
     *
     * @return 如果完成则返回 true，否则返回 false。
     */
    boolean isFinished();

    /**
     * 告诉我们是否消费了所有可用的数据。
     *
     * <p>此外，它告诉我们为什么没有更多的数据传入。如果上游任何子任务因为停止带保存点而停止（--no-drain），我们就不应该消耗输入。
     * 详见 {@code StopMode}。
     * @return 当前数据输入的结束状态。
     */
    EndOfDataStatus hasReceivedEndOfData();

    /** 用于描述是否已经到达数据结束的状态。 */
    enum EndOfDataStatus {
        NOT_END_OF_DATA, // 尚未到达数据结束
        DRAINED,         // 数据已经完全被消耗
        STOPPED          // 因为停止命令而停止
    }
}
