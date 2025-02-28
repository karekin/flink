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

package org.apache.flink.table.connector.source.abilities;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.connector.source.ScanTableSource;

import java.util.List;

@PublicEvolving
public interface SupportsDynamicFiltering {

    /**
     * 返回该分区表源支持的过滤字段列表。
     * <p>
     * 此方法告诉规划器哪些字段可以作为动态过滤字段使用，
     * 规划器将根据查询从返回的字段中选择一些字段，并创建动态过滤操作符。
     * <p>
     * 例如，假设有一个查询：
     *
     * <pre>
     * SELECT * FROM partitioned_fact_table t1, non_partitioned_dim_table t2
     *   WHERE t1.part_key = t2.col1 AND t2.col2 = 100
     * </pre>
     *
     * 在这个查询中，`partitioned_fact_table` 是一个分区表，其分区键为 `part_key`，
     * 而 `non_partitioned_dim_table` 是一个非分区表，其数据包含 `partitioned_fact_table` 的所有分区值。
     * 通过过滤条件 `t2.col2 = 100`，只有部分分区需要被扫描以进行连接操作。
     * 具体需要扫描哪些分区在优化阶段是不可用的，而是在执行阶段才可用。
     * <p>
     * 如果没有实现此接口，数据源会先读取所有数据，然后再进行过滤操作。
     * 如果实现了此接口，那么源需要告知规划器它支持哪些过滤字段，规划器会根据查询选择合适的字段，
     * 并在执行时通过动态过滤操作符将数据发送到源。
     * <p>
     * 未来，可以通过这个接口将更灵活的过滤条件推入源连接器中。
     *
     * @return 支持的过滤字段列表
     */
    List<String> listAcceptedFilterFields();

    /**
     * 将候选过滤字段应用到表源中。
     * <p>
     * 在运行时，与过滤字段对应的数据将被提供，可以用于过滤分区或输入数据。
     * <p>
     * <b>注意：</b> 候选过滤字段始终来自 {@link #listAcceptedFilterFields()} 的返回结果。
     *
     * @param candidateFilterFields 候选过滤字段列表
     */
    void applyDynamicFiltering(List<String> candidateFilterFields);
}
