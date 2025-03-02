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

package org.apache.flink.table.functions;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * 异步查找函数的包装类，用于异步查找与查询键匹配的行。
 * 该类的输出类型固定为 {@link RowData}。
 */
@PublicEvolving
public abstract class AsyncLookupFunction extends AsyncTableFunction<RowData> {

    /**
     * 异步查找与查询键匹配的行。
     * <p>
     * 注意：返回的行集合 `RowData` 不能跨调用复用。
     * </p>
     * @param keyRow 包含查询键的 `RowData`。
     * @return 返回包含所有匹配行的 `CompletableFuture`。
     */
    public abstract CompletableFuture<Collection<RowData>> asyncLookup(RowData keyRow);

    /**
     * 调用 {@link #asyncLookup} 方法并链接 `CompletableFuture`。
     * <p>
     * 此方法用于将异步查找结果传递给外部的 `CompletableFuture`。
     * 如果查找过程中发生异常，会将异常包装为 `TableException` 并传递给外部的 `CompletableFuture`。
     * </p>
     * @param future 用于接收异步查找结果的 `CompletableFuture`。
     * @param keys 查询键。
     */
    public final void eval(CompletableFuture<Collection<RowData>> future, Object... keys) {
        // 将输入的查询键转换为 `RowData` 格式
        GenericRowData keyRow = GenericRowData.of(keys);

        // 调用抽象方法 `asyncLookup` 进行异步查找
        asyncLookup(keyRow)
                .whenComplete(
                        (result, exception) -> {
                            if (exception != null) {
                                // 如果发生异常，构造 `TableException` 并传递给外部的 `CompletableFuture`
                                future.completeExceptionally(
                                        new TableException(
                                                String.format(
                                                        "Failed to asynchronously lookup entries with key '%s'",
                                                        keyRow),
                                                exception));
                                return;
                            }
                            // 如果查找成功，将结果传递给外部的 `CompletableFuture`
                            future.complete(result);
                        });
    }
}
