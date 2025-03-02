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

package org.apache.flink.table.connector.sink.abilities;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.connector.sink.DynamicTableSink;

/**
 * 允许在 {@link DynamicTableSink} 中覆盖已有数据的接口。
 *
 * <p>默认情况下，如果未实现此接口，则无法使用 SQL 语句（例如 {@code INSERT OVERWRITE}）覆盖已有的表或分区数据。
 */
@PublicEvolving
public interface SupportsOverwrite {

    /**
     * 指定是否应覆盖现有数据。
     *
     * @param overwrite 如果为 true，则允许覆盖现有数据；如果为 false，则不覆盖现有数据。
     */
    void applyOverwrite(boolean overwrite);
}
