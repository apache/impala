// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "gen-cpp/TCLIService_types.h"
#include "gen-cpp/CatalogService_types.h"
#include "gen-cpp/CatalogObjects_types.h"

#include <vector>

namespace impala {

/// Populates the supplied TAlterTableUpdateStatsParams argument with per-column statistics
/// that are computed by aggregating the per-partition results from the COMPUTE STATS child
/// queries, contained in the 'rowset', and any pre-existing per-partition measurements,
/// contained in 'existing_part_stats'. The schema for 'rowset' is in 'col_stats_schema'.
void FinalizePartitionedColumnStats(
    const apache::hive::service::cli::thrift::TTableSchema& col_stats_schema,
    const std::vector<TPartitionStats>& existing_part_stats,
    const std::vector<std::vector<std::string>>& expected_partitions,
    const apache::hive::service::cli::thrift::TRowSet& rowset,
    int32_t num_partition_cols, TAlterTableUpdateStatsParams* params);
}
